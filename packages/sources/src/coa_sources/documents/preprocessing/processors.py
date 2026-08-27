# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""File-type-specific document processors for the preprocessing Lambda.

Each processor converts a file's raw bytes into clean text suitable for
the GraphRAG Toolkit.  Processors use lazy imports so that heavy
dependencies (``unstructured``, ``pypdfium2``) are only loaded when the
corresponding code path is hit, keeping cold-start time low for
pass-through formats.
"""

from __future__ import annotations

import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)

# Chars-per-page threshold below which a PDF is considered scanned (image-based).
# PDFs with fewer than this many characters per page on average are routed to Textract.
_SCANNED_PDF_THRESHOLD = 50

# DPI used when rendering PDF pages to PNG for Textract OCR.
# 300 DPI is the recommended minimum for accurate OCR results.
_TEXTRACT_RENDER_DPI = 300

# Hard caps on what we will rasterize. A PDF is untrusted input: a few thousand
# pages, or a single page with absurd dimensions, compresses to almost nothing on
# disk but expands to gigabytes of bitmap once rendered at 300 DPI — enough to OOM
# the task. Exceeding either cap raises a clear error (reported per-file by the
# caller) instead of being silently truncated.
_MAX_PDF_PAGES = 1000

# Per-page pixel budget at the render DPI. US-Letter at 300 DPI is ~8.4 MP, so 50 MP
# leaves generous headroom for large-format pages while rejecting pathological ones
# (a 100000x100000 pt page would be ~173,000 MP).
_MAX_RENDER_MEGAPIXELS = 50

# Concurrent Textract DetectDocumentText calls per document. Textract sync APIs
# allow concurrent requests up to an account TPS quota (default ~10 TPS); boto3
# clients are thread-safe for calls, so we fan the per-page OCR calls out over a
# thread pool. Tunable via env; keep <= the account Textract TPS to avoid
# throttling (boto retries throttles, but staying under is cheaper).
_TEXTRACT_CONCURRENCY = int(os.environ.get("TEXTRACT_CONCURRENCY", "10"))


# ---------------------------------------------------------------------------
# Pass-through processors
# ---------------------------------------------------------------------------


def process_txt(content_bytes: bytes, filename: str) -> tuple[str, str]:
    """Pass through ``.txt`` files (UTF-8 decode only)."""
    return content_bytes.decode("utf-8", errors="replace"), ".txt"


def process_md(content_bytes: bytes, filename: str) -> tuple[str, str]:
    """Pass through ``.md`` files (UTF-8 decode only)."""
    return content_bytes.decode("utf-8", errors="replace"), ".md"


# ---------------------------------------------------------------------------
# DOCX processor
# ---------------------------------------------------------------------------


def process_docx(content_bytes: bytes, filename: str) -> tuple[str, str]:
    """Convert ``.docx`` to Markdown via ``unstructured.partition_docx``."""
    from unstructured.partition.docx import partition_docx  # lazy import

    logger.info(
        "Processing DOCX with unstructured",
        extra={"doc_filename": filename},
    )
    elements = partition_docx(file=io.BytesIO(content_bytes))
    text = elements_to_markdown(elements)
    # WARNING on an empty result — see the note in process_pdf.
    log = logger.warning if not text.strip() else logger.info
    log(
        "DOCX processed",
        extra={"doc_filename": filename, "elements": len(elements), "char_count": len(text)},
    )
    return text, ".md"


# ---------------------------------------------------------------------------
# PDF processor (auto-detects scanned vs text-native)
# ---------------------------------------------------------------------------


def process_pdf(
    content_bytes: bytes,
    filename: str,
    textract_client: Any,
) -> tuple[str, str]:
    """Process a PDF — text-native via unstructured, scanned via Textract."""
    if is_scanned_pdf(content_bytes):
        logger.info(
            "PDF detected as scanned, routing to Textract",
            extra={"doc_filename": filename},
        )
        text = process_pdf_textract(content_bytes, filename, textract_client)
        return text, ".txt"

    logger.info(
        "PDF detected as text-native, using unstructured",
        extra={"doc_filename": filename},
    )
    from unstructured.partition.pdf import partition_pdf  # lazy import

    # strategy="fast" uses pdfminer only — no layout detection models (torch/detectron2)
    elements = partition_pdf(file=io.BytesIO(content_bytes), strategy="fast")
    text = elements_to_markdown(elements)
    # WARNING, not INFO, on an empty result: `unstructured` gives up on
    # drawing-heavy pages and returns zero characters without raising, so an
    # extraction that found nothing and one that worked used to be the same
    # severity — log-based alerting could not tell them apart.
    log = logger.warning if not text.strip() else logger.info
    log(
        "PDF processed",
        extra={"doc_filename": filename, "elements": len(elements), "char_count": len(text)},
    )
    return text, ".md"


def is_scanned_pdf(pdf_bytes: bytes) -> bool:
    """Return True if the PDF appears to be scanned (< 50 chars/page avg).

    A PDF that cannot be opened or whose text cannot be extracted is reported as
    scanned rather than raising, so the caller gets a usable verdict and a logged
    reason instead of a PDFium traceback.
    """
    import pypdfium2 as pdfium  # lazy import

    try:
        doc = pdfium.PdfDocument(pdf_bytes)
    except pdfium.PdfiumError as exc:
        logger.warning(
            "Could not open PDF for scanned detection; treating as scanned",
            extra={"reason": str(exc)},
        )
        return True

    try:
        num_pages = len(doc)
        if num_pages == 0:
            return True

        total_chars = 0
        for page_num in range(num_pages):
            # Close each textpage explicitly: it wraps a native PDFium handle, and
            # relying on GC to reclaim them leaks native memory across a long
            # multi-page document.
            textpage = doc[page_num].get_textpage()
            try:
                total_chars += len(textpage.get_text_range())
            finally:
                textpage.close()
        avg_chars = total_chars / num_pages
    except pdfium.PdfiumError as exc:
        logger.warning(
            "Could not extract PDF text for scanned detection; treating as scanned",
            extra={"reason": str(exc)},
        )
        return True
    finally:
        doc.close()

    logger.info(
        "Scanned PDF detection",
        extra={
            "total_chars": total_chars,
            "avg_chars_per_page": round(avg_chars, 1),
            "threshold": _SCANNED_PDF_THRESHOLD,
            "page_count": num_pages,
        },
    )
    return avg_chars < _SCANNED_PDF_THRESHOLD


def process_pdf_textract(
    pdf_bytes: bytes,
    filename: str,
    textract_client: Any,
) -> str:
    """Extract text from a scanned PDF using Amazon Textract.

    Renders each page to a 300-DPI PNG via pypdfium2, then OCRs the pages with
    Textract sync ``DetectDocumentText``. Rendering is done sequentially first
    (PDFium is NOT thread-safe), then the per-page Textract calls are fanned
    out over a thread pool (``_TEXTRACT_CONCURRENCY``) since boto3 clients are
    thread-safe and Textract accepts concurrent requests. Results are
    reassembled in page order.

    Raises:
        ValueError: if the PDF exceeds ``_MAX_PDF_PAGES`` or a page would rasterize
            to more than ``_MAX_RENDER_MEGAPIXELS`` at the render DPI. Both are
            resource guards against untrusted input; the message is safe to surface.
    """
    import pypdfium2 as pdfium  # lazy import

    try:
        doc = pdfium.PdfDocument(pdf_bytes)
    except pdfium.PdfiumError as exc:
        logger.warning(
            "Could not open PDF for Textract rendering",
            extra={"doc_filename": filename, "reason": str(exc)},
        )
        return ""

    try:
        num_pages = len(doc)
        # Check the page count immediately after opening: rendering is what costs
        # memory, so reject before the first bitmap is allocated.
        if num_pages > _MAX_PDF_PAGES:
            raise ValueError(f"PDF has {num_pages} pages, exceeding the {_MAX_PDF_PAGES}-page limit for OCR")

        scale = _TEXTRACT_RENDER_DPI / 72
        max_pixels = _MAX_RENDER_MEGAPIXELS * 1_000_000

        # 1. Render all pages to PNG sequentially (local CPU; PDFium not
        #    thread-safe), keeping page order. scale 1.0 == 72 DPI.
        page_pngs: list[bytes] = []
        for page_num in range(num_pages):
            page = doc[page_num]

            # Validate dimensions BEFORE rendering — the bitmap is allocated inside
            # render(), so an oversized page must be rejected up front.
            width_pt, height_pt = page.get_size()
            pixels = (width_pt * scale) * (height_pt * scale)
            if pixels > max_pixels:
                raise ValueError(
                    f"PDF page {page_num + 1} is {width_pt:.0f}x{height_pt:.0f} pt, which renders to "
                    f"{pixels / 1_000_000:.0f} MP at {_TEXTRACT_RENDER_DPI} DPI, "
                    f"exceeding the {_MAX_RENDER_MEGAPIXELS} MP per-page limit"
                )

            try:
                bitmap = page.render(scale=scale)
            except pdfium.PdfiumError as exc:
                logger.warning(
                    "Could not render PDF page for Textract",
                    extra={"doc_filename": filename, "page": page_num + 1, "reason": str(exc)},
                )
                return ""
            try:
                buf = io.BytesIO()
                # to_pil() views the bitmap buffer, so serialize before releasing it.
                bitmap.to_pil().save(buf, format="PNG")
                page_pngs.append(buf.getvalue())
            finally:
                # Release the native bitmap now rather than waiting for GC; at
                # 300 DPI each page is tens of MB and they would otherwise
                # accumulate across the whole document.
                bitmap.close()
    finally:
        doc.close()

    total_pages = len(page_pngs)

    def _ocr_page(idx_png: tuple[int, bytes]) -> tuple[int, str]:
        idx, png_bytes = idx_png
        logger.info(
            "Calling Textract for page",
            extra={"doc_filename": filename, "page": idx + 1, "total_pages": total_pages},
        )
        resp = textract_client.detect_document_text(Document={"Bytes": png_bytes})
        lines = [block["Text"] for block in resp.get("Blocks", []) if block["BlockType"] == "LINE"]
        return idx, "\n".join(lines)

    # 2. OCR pages concurrently (I/O-bound remote calls); reassemble in order.
    pages_text: list[str] = [""] * total_pages
    if total_pages:
        workers = min(_TEXTRACT_CONCURRENCY, total_pages)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for idx, text in pool.map(_ocr_page, enumerate(page_pngs)):
                pages_text[idx] = text

    return "\n\n".join(pages_text)


# ---------------------------------------------------------------------------
# Element-to-Markdown conversion
# ---------------------------------------------------------------------------


def elements_to_markdown(elements: list) -> str:
    """Convert unstructured ``Element`` objects to a Markdown string."""
    lines: list[str] = []

    for el in elements:
        category = el.category if hasattr(el, "category") else ""
        text = str(el)

        if category == "Title":
            lines.append(f"## {text}")
        elif category == "Header":
            lines.append(f"# {text}")
        elif category == "ListItem":
            lines.append(f"- {text}")
        elif category == "Table":
            lines.append(f"```\n{text}\n```")
        elif text.strip():
            lines.append(text)

    return "\n\n".join(lines)


# ---------------------------------------------------------------------------
# Page count helper
# ---------------------------------------------------------------------------


def get_page_count(content_bytes: bytes, ext: str) -> int:
    """Return the page count for PDFs; 0 for other formats."""
    if ext != ".pdf":
        return 0
    import pypdfium2 as pdfium  # lazy import

    try:
        doc = pdfium.PdfDocument(content_bytes)
    except pdfium.PdfiumError as exc:
        # Page count is metadata, not the extraction itself — a malformed PDF
        # reports 0 pages here and is handled by the processing path, which
        # surfaces the failure with a filename-scoped message.
        logger.warning("Could not read PDF page count", extra={"reason": str(exc)})
        return 0

    try:
        return len(doc)
    finally:
        doc.close()


# ---------------------------------------------------------------------------
# Processor dispatch table
# ---------------------------------------------------------------------------

_PROCESSORS: dict[str, Any] = {
    ".txt": process_txt,
    ".md": process_md,
    ".docx": process_docx,
    # .pdf is handled separately in handler.py (needs textract_client)
}
