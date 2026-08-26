# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the preprocessing Lambda processors module."""

import io
import logging
import pathlib
import sys
import time
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

for _mod_name in (
    "unstructured",
    "unstructured.partition",
):
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = ModuleType(_mod_name)

# These need a callable attribute so @patch can target it
for _mod_name, _attr in [
    ("unstructured.partition.docx", "partition_docx"),
    ("unstructured.partition.pdf", "partition_pdf"),
]:
    if _mod_name not in sys.modules:
        mod = ModuleType(_mod_name)
        setattr(mod, _attr, None)
        sys.modules[_mod_name] = mod

from coa_sources.documents.preprocessing.processors import (
    _PROCESSORS,
    _SCANNED_PDF_THRESHOLD,
    elements_to_markdown,
    get_page_count,
    process_docx,
    process_md,
    process_pdf,
    process_pdf_textract,
    process_txt,
)

# ---------------------------------------------------------------------------
# Pass-through processors
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestProcessTxt:
    def test_decodes_utf8(self):
        text, ext = process_txt(b"hello world", "file.txt")
        assert text == "hello world"
        assert ext == ".txt"

    def test_preserves_newlines(self):
        text, _ = process_txt(b"line1\nline2\nline3", "file.txt")
        assert text == "line1\nline2\nline3"

    def test_replaces_invalid_bytes(self):
        text, ext = process_txt(b"hello \xff world", "file.txt")
        assert "hello" in text
        assert "world" in text
        assert ext == ".txt"

    def test_empty_file(self):
        text, ext = process_txt(b"", "empty.txt")
        assert text == ""
        assert ext == ".txt"


@pytest.mark.unit
class TestProcessMd:
    def test_decodes_utf8(self):
        text, ext = process_md(b"# Heading\n\nParagraph", "doc.md")
        assert text == "# Heading\n\nParagraph"
        assert ext == ".md"

    def test_replaces_invalid_bytes(self):
        text, _ = process_md(b"# Title\xff", "doc.md")
        assert "Title" in text

    def test_empty_file(self):
        text, ext = process_md(b"", "empty.md")
        assert text == ""
        assert ext == ".md"


# ---------------------------------------------------------------------------
# DOCX processor
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestProcessDocx:
    @patch("unstructured.partition.docx.partition_docx")
    def test_calls_partition_and_returns_markdown(self, mock_partition):
        el = MagicMock()
        el.category = "NarrativeText"
        el.__str__ = MagicMock(return_value="Some paragraph text")
        mock_partition.return_value = [el]

        text, ext = process_docx(b"fake-docx-bytes", "doc.docx")
        assert ext == ".md"
        assert "Some paragraph text" in text
        mock_partition.assert_called_once()

    @patch("unstructured.partition.docx.partition_docx")
    def test_empty_docx(self, mock_partition):
        mock_partition.return_value = []
        text, ext = process_docx(b"fake", "empty.docx")
        assert text == ""
        assert ext == ".md"


# ---------------------------------------------------------------------------
# Scanned PDF detection
# ---------------------------------------------------------------------------


def _make_pdf(pages_text: list[str]) -> bytes:
    """Build a minimal valid PDF (US-Letter, Helvetica 12pt, one text line per page).

    Real bytes, parsed by the real PDF library — no mocks. pdfium extracts a
    single-line ``Tj`` string verbatim, so char-count-sensitive tests (the
    scanned-PDF threshold) are exact.
    """
    objects: list[bytes] = []
    n_pages = len(pages_text)
    page_obj_nums = [4 + 2 * i for i in range(n_pages)]
    kids = " ".join(f"{n} 0 R" for n in page_obj_nums)
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")  # obj 1
    objects.append(f"<< /Type /Pages /Kids [{kids}] /Count {n_pages} >>".encode())  # obj 2
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")  # obj 3
    for i, text in enumerate(pages_text):
        content_num = page_obj_nums[i] + 1
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_num} 0 R >>"
            ).encode()
        )
        esc = text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")
        stream = f"BT /F1 12 Tf 72 720 Td ({esc}) Tj ET".encode()
        objects.append(b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream")

    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = []
    for num, body in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(f"{num} 0 obj\n".encode() + body + b"\nendobj\n")
    xref_pos = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode())
    out.write(b"0000000000 65535 f \n")
    for off in offsets:
        out.write(f"{off:010d} 00000 n \n".encode())
    out.write(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode())
    return out.getvalue()


@pytest.mark.unit
class TestIsScannedPdf:
    def test_text_heavy_pdf_not_scanned(self):
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        assert is_scanned_pdf(_make_pdf(["a" * 500, "b" * 600])) is False

    def test_image_only_pdf_is_scanned(self):
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        assert is_scanned_pdf(_make_pdf(["", ""])) is True

    def test_below_threshold_is_scanned(self):
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        assert is_scanned_pdf(_make_pdf(["x" * (_SCANNED_PDF_THRESHOLD - 1)])) is True

    def test_at_threshold_not_scanned(self):
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        assert is_scanned_pdf(_make_pdf(["x" * _SCANNED_PDF_THRESHOLD])) is False

    def test_corrupt_pdf_treated_as_scanned(self):
        """Unopenable bytes must not raise — report scanned and log the reason."""
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        assert is_scanned_pdf(b"%PDF-1.4\nnot actually a pdf") is True

    def test_truncated_pdf_treated_as_scanned(self):
        """The repo's truncated.pdf edge-case fixture must not raise."""
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        fixture = (
            pathlib.Path(__file__).parents[6] / "tests/cdk/scripts/preprocessing-fixtures/edge-cases/truncated.pdf"
        )
        if not fixture.is_file():  # fixture lives at repo root; skip if packaged alone
            pytest.skip(f"fixture not present: {fixture}")
        assert is_scanned_pdf(fixture.read_bytes()) is True

    def test_empty_bytes_treated_as_scanned(self):
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        assert is_scanned_pdf(b"") is True

    def test_corrupt_pdf_logs_reason(self, caplog):
        from coa_sources.documents.preprocessing.processors import is_scanned_pdf

        with caplog.at_level(logging.WARNING):
            is_scanned_pdf(b"garbage")
        assert any("treating as scanned" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Textract extraction
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestProcessPdfTextract:
    def test_single_page_calls_textract_with_real_png(self):
        textract = MagicMock()
        textract.detect_document_text.return_value = {
            "Blocks": [
                {"BlockType": "LINE", "Text": "Hello from Textract"},
                {"BlockType": "WORD", "Text": "ignored"},
            ]
        }

        result = process_pdf_textract(_make_pdf(["hello"]), "scan.pdf", textract)
        assert result == "Hello from Textract"
        textract.detect_document_text.assert_called_once()

        # The bytes handed to Textract are a real PNG at 300 DPI:
        # US-Letter 612x792 pt -> 2550x3300 px (pdfium may round up by 1px).
        png_bytes = textract.detect_document_text.call_args.kwargs["Document"]["Bytes"]
        assert png_bytes[:8] == b"\x89PNG\r\n\x1a\n"
        from PIL import Image

        width, height = Image.open(io.BytesIO(png_bytes)).size
        assert abs(width - 2550) <= 1 and abs(height - 3300) <= 1

    def test_multi_page_joins_with_double_newline(self):
        pdf_bytes = _make_pdf(["alpha", "beta", "gamma"])

        # Pre-render each page with the same library/scale to build a
        # bytes -> page-index map (rendering is deterministic), so the Textract
        # stub can identify pages by content, not call order.
        import pypdfium2 as pdfium

        doc = pdfium.PdfDocument(pdf_bytes)
        expected_pngs: dict[bytes, int] = {}
        try:
            for i in range(len(doc)):
                buf = io.BytesIO()
                doc[i].render(scale=300 / 72).to_pil().save(buf, format="PNG")
                expected_pngs[buf.getvalue()] = i
        finally:
            doc.close()
        assert len(expected_pngs) == 3, "pages must render to distinct PNGs"

        # process_pdf_textract OCRs pages concurrently, so the stub must key off
        # the page bytes, not call order: a side_effect *list* is consumed in the
        # order threads happen to call, which makes the assertion below flaky.
        # Do not "simplify" this back into a list.
        # The descending sleep makes pages finish in reverse order, so the
        # assertion deterministically catches a reassembly that appends in
        # completion order instead of indexing by page.
        def _detect(Document):  # noqa: N803 - matches the boto3 kwarg name
            idx = expected_pngs[Document["Bytes"]]
            time.sleep(0.05 * (3 - idx))
            return {"Blocks": [{"BlockType": "LINE", "Text": f"Page {idx + 1}"}]}

        textract = MagicMock()
        textract.detect_document_text.side_effect = _detect

        result = process_pdf_textract(pdf_bytes, "multi.pdf", textract)
        assert result == "Page 1\n\nPage 2\n\nPage 3"
        assert textract.detect_document_text.call_count == 3

    def test_empty_textract_response(self):
        textract = MagicMock()
        textract.detect_document_text.return_value = {"Blocks": []}

        result = process_pdf_textract(_make_pdf(["scan"]), "empty.pdf", textract)
        assert result == ""

    def test_corrupt_pdf_returns_empty_without_calling_textract(self):
        """An unopenable PDF must not raise a PDFium error nor bill Textract."""
        textract = MagicMock()
        assert process_pdf_textract(b"%PDF-1.4 garbage", "bad.pdf", textract) == ""
        textract.detect_document_text.assert_not_called()

    def test_page_count_limit_enforced_before_rendering(self):
        """Over-long PDFs are rejected with a clear message, before any render."""
        from coa_sources.documents.preprocessing import processors

        textract = MagicMock()
        with (
            patch.object(processors, "_MAX_PDF_PAGES", 2),
            pytest.raises(ValueError, match="exceeding the 2-page limit"),
        ):
            processors.process_pdf_textract(_make_pdf(["a", "b", "c"]), "long.pdf", textract)
        textract.detect_document_text.assert_not_called()

    def test_oversized_page_rejected_before_rendering(self):
        """A page too large to rasterize safely is rejected, not rendered."""
        from coa_sources.documents.preprocessing import processors

        textract = MagicMock()
        # US-Letter at 300 DPI is ~8.4 MP; a 1 MP cap rejects it without allocating.
        with (
            patch.object(processors, "_MAX_RENDER_MEGAPIXELS", 1),
            pytest.raises(ValueError, match="exceeding the 1 MP per-page limit"),
        ):
            processors.process_pdf_textract(_make_pdf(["big"]), "big.pdf", textract)
        textract.detect_document_text.assert_not_called()

    def test_within_page_limit_still_processes(self):
        """The cap must not reject documents at or under the limit."""
        from coa_sources.documents.preprocessing import processors

        textract = MagicMock()
        textract.detect_document_text.return_value = {"Blocks": [{"BlockType": "LINE", "Text": "ok"}]}
        with patch.object(processors, "_MAX_PDF_PAGES", 2):
            result = processors.process_pdf_textract(_make_pdf(["a", "b"]), "two.pdf", textract)
        assert result == "ok\n\nok"


# ---------------------------------------------------------------------------
# PDF processor (routing logic)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestProcessPdf:
    @patch("unstructured.partition.pdf.partition_pdf")
    @patch("coa_sources.documents.preprocessing.processors.is_scanned_pdf", return_value=False)
    def test_text_native_uses_unstructured(self, mock_scanned, mock_partition):
        el = MagicMock()
        el.category = "NarrativeText"
        el.__str__ = MagicMock(return_value="PDF text content")
        mock_partition.return_value = [el]

        text, ext = process_pdf(b"pdf-bytes", "doc.pdf", MagicMock())
        assert ext == ".md"
        assert "PDF text content" in text
        mock_partition.assert_called_once()

    @patch(
        "coa_sources.documents.preprocessing.processors.process_pdf_textract",
        return_value="Textract output",
    )
    @patch("coa_sources.documents.preprocessing.processors.is_scanned_pdf", return_value=True)
    def test_scanned_uses_textract(self, mock_scanned, mock_textract):
        textract_client = MagicMock()
        text, ext = process_pdf(b"pdf-bytes", "scan.pdf", textract_client)
        assert ext == ".txt"
        assert text == "Textract output"
        mock_textract.assert_called_once_with(b"pdf-bytes", "scan.pdf", textract_client)

    @patch("unstructured.partition.pdf.partition_pdf")
    @patch("coa_sources.documents.preprocessing.processors.is_scanned_pdf", return_value=False)
    def test_empty_result_logs_at_warning(self, mock_scanned, mock_partition, caplog):
        """An extraction that found nothing must not log like one that worked.

        `unstructured` abandons drawing-heavy pages and returns no elements without
        raising. At INFO, char_count=0 and char_count=3000 were indistinguishable to
        log-based alerting.
        """
        mock_partition.return_value = []

        with caplog.at_level(logging.INFO):
            text, ext = process_pdf(b"pdf-bytes", "drawing.pdf", MagicMock())

        assert text == ""
        records = [r for r in caplog.records if r.getMessage() == "PDF processed"]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert records[0].char_count == 0

    @patch("unstructured.partition.pdf.partition_pdf")
    @patch("coa_sources.documents.preprocessing.processors.is_scanned_pdf", return_value=False)
    def test_non_empty_result_stays_at_info(self, mock_scanned, mock_partition, caplog):
        el = MagicMock()
        el.category = "NarrativeText"
        el.__str__ = MagicMock(return_value="Real content")
        mock_partition.return_value = [el]

        with caplog.at_level(logging.INFO):
            process_pdf(b"pdf-bytes", "doc.pdf", MagicMock())

        records = [r for r in caplog.records if r.getMessage() == "PDF processed"]
        assert len(records) == 1
        assert records[0].levelno == logging.INFO


# ---------------------------------------------------------------------------
# elements_to_markdown
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestElementsToMarkdown:
    def _make_element(self, category: str, text: str) -> MagicMock:
        el = MagicMock()
        el.category = category
        el.__str__ = MagicMock(return_value=text)
        return el

    def test_title_becomes_h2(self):
        result = elements_to_markdown([self._make_element("Title", "My Title")])
        assert result == "## My Title"

    def test_header_becomes_h1(self):
        result = elements_to_markdown([self._make_element("Header", "My Header")])
        assert result == "# My Header"

    def test_list_item_becomes_bullet(self):
        result = elements_to_markdown([self._make_element("ListItem", "First item")])
        assert result == "- First item"

    def test_table_becomes_code_block(self):
        result = elements_to_markdown([self._make_element("Table", "col1 | col2")])
        assert result == "```\ncol1 | col2\n```"

    def test_narrative_text_plain(self):
        result = elements_to_markdown([self._make_element("NarrativeText", "Paragraph text")])
        assert result == "Paragraph text"

    def test_empty_text_skipped(self):
        result = elements_to_markdown([self._make_element("NarrativeText", "   ")])
        assert result == ""

    def test_mixed_elements(self):
        els = [
            self._make_element("Header", "Doc Title"),
            self._make_element("NarrativeText", "Introduction paragraph"),
            self._make_element("Title", "Section 1"),
            self._make_element("ListItem", "Item A"),
            self._make_element("ListItem", "Item B"),
        ]
        result = elements_to_markdown(els)
        lines = result.split("\n\n")
        assert lines[0] == "# Doc Title"
        assert lines[1] == "Introduction paragraph"
        assert lines[2] == "## Section 1"
        assert lines[3] == "- Item A"
        assert lines[4] == "- Item B"

    def test_empty_list(self):
        assert elements_to_markdown([]) == ""

    def test_no_category_attribute(self):
        """Element without a .category attribute falls through to plain text."""

        class BareElement:
            """An element with no .category attribute."""

            def __str__(self):
                return "plain text"

        el = BareElement()
        result = elements_to_markdown([el])
        assert result == "plain text"


# ---------------------------------------------------------------------------
# get_page_count
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetPageCount:
    def test_pdf_returns_count(self):
        assert get_page_count(_make_pdf(["p1", "p2", "p3", "p4", "p5"]), ".pdf") == 5

    def test_malformed_pdf_returns_zero(self):
        """A malformed PDF reports 0 pages rather than raising a PDFium error."""
        assert get_page_count(b"%PDF-1.4 not a pdf", ".pdf") == 0

    def test_empty_bytes_returns_zero(self):
        assert get_page_count(b"", ".pdf") == 0

    def test_txt_returns_zero(self):
        assert get_page_count(b"text", ".txt") == 0

    def test_md_returns_zero(self):
        assert get_page_count(b"# heading", ".md") == 0

    def test_docx_returns_zero(self):
        assert get_page_count(b"docx-bytes", ".docx") == 0


# ---------------------------------------------------------------------------
# Processor dispatch table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestProcessorDispatchTable:
    def test_has_txt(self):
        assert ".txt" in _PROCESSORS
        assert _PROCESSORS[".txt"] is process_txt

    def test_has_md(self):
        assert ".md" in _PROCESSORS
        assert _PROCESSORS[".md"] is process_md

    def test_has_docx(self):
        assert ".docx" in _PROCESSORS
        assert _PROCESSORS[".docx"] is process_docx

    def test_pdf_not_in_table(self):
        assert ".pdf" not in _PROCESSORS

    def test_no_unexpected_keys(self):
        assert set(_PROCESSORS.keys()) == {".txt", ".md", ".docx"}
