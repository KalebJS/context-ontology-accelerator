// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stripping a customer-authored tag out of a Unity Catalog comment, and leaving alone everything COA
 * leaves alone.
 */
class CommentTagsTest
{
    @Test
    void stripsALiveForeignKeyTag()
    {
        // Anyone with MODIFY can run COMMENT ON COLUMN. Left in, this asserts a relationship Unity Catalog
        // never declared, into a table that may not exist.
        assertEquals("Line total",
                CommentTags.strip("Line total @fk(payroll.ssn)"));
    }

    @Test
    void stripsALivePrimaryKeyTag()
    {
        assertEquals("Customer surrogate key",
                CommentTags.strip("Customer surrogate key @pk"));
    }

    @Test
    void stripsSeveralTagsAndKeepsTheProseAroundThem()
    {
        assertEquals("before middle after",
                CommentTags.strip("before @pk middle @fk(orders.order_id) after"));
    }

    @Test
    void stripsATagAtTheStart()
    {
        assertEquals("the rest", CommentTags.strip("@pk the rest"));
        assertEquals("the rest", CommentTags.strip("@fk(a.b) the rest"));
    }

    @Test
    void leavesNothingWhenTheCommentWasOnlyTags()
    {
        assertEquals("", CommentTags.strip("@pk @fk(orders.order_id)"));
    }

    @Test
    void anEmailAddressDoesNotLookLikeATag()
    {
        // The left-boundary rule: without it an address in a comment mints a primary key. This is the live
        // fixture's own order_lines.sku comment.
        assertEquals("Contact bob@pk.example.com about this column",
                CommentTags.strip("Contact bob@pk.example.com about this @pk column"));
    }

    @Test
    void leavesNearMissesAloneBecauseCoaLeavesThemAlone()
    {
        // Case is significant, and @pk takes no operand: @pk=x and @pk(x) are spellings COA reports and
        // keeps. Removing them here would delete text a customer wrote and COA would have stored.
        for (String prose : new String[] {
            "value @PK here", "value @pkey here", "value @pk=x here", "value @pk(x) here",
            "value @FK(a.b) here", "value @fkey(a.b) here"}) {
            assertEquals(prose, CommentTags.strip(prose), prose);
        }
    }

    @Test
    void aForeignKeyOperandMayContainABracketInsideQuotes()
    {
        // The grammar quotes per segment, and a ")" inside a quoted segment is data. Getting the extent
        // wrong leaves half a tag in the prose.
        assertEquals("after", CommentTags.strip("@fk(\"a)b\".c) after"));
        assertEquals("after", CommentTags.strip("@fk(\"say \"\"hi\"\")\".c) after"));
    }

    @Test
    void anUnterminatedForeignKeyTagIsLeftAlone()
    {
        // COA reports a malformed tag and keeps it in the stored description, and that surviving text is
        // the only feedback its author gets.
        assertEquals("Line total @fk(orders.order_id",
                CommentTags.strip("Line total @fk(orders.order_id"));
    }

    @Test
    void collapsesTheWhitespaceATagLeavesBehind()
    {
        assertEquals("a b", CommentTags.strip("a   @pk    b"));
        assertEquals("a b", CommentTags.strip("a\n@pk\tb"));
    }

    @Test
    void handlesNullAndEmpty()
    {
        assertEquals("", CommentTags.strip(null));
        assertEquals("", CommentTags.strip(""));
        assertEquals("", CommentTags.strip("   "));
    }

    @Test
    void carriesTagReportsWhetherAnythingWouldBeStripped()
    {
        assertTrue(CommentTags.carriesTag("Line total @pk"));
        assertTrue(CommentTags.carriesTag("Line total @fk(a.b)"));
        assertFalse(CommentTags.carriesTag("Line total"));
        assertFalse(CommentTags.carriesTag("bob@pk.example.com"));
        assertFalse(CommentTags.carriesTag(null));
    }

    @Test
    void aStrippedCommentIsAcceptableToTheToolkitsEncoder()
    {
        // ColumnComment.of() throws on prose carrying a live tag, so any comment this class returns has to
        // pass it. Otherwise a customer's tag fails the whole table's describe rather than being ignored.
        dev.coa.connector.constraints.ColumnComment.of(
                CommentTags.strip("prose @pk more @fk(orders.order_id) end"));
        dev.coa.connector.constraints.ColumnComment.of(
                CommentTags.strip("Contact bob@pk.example.com about this @pk column"));
    }

    // ---------------------------------------------------------------------------------------------
    // neutralise: what strip() deliberately leaves behind, and the encoder refuses
    // ---------------------------------------------------------------------------------------------

    @Test
    void stripAloneLeavesAnUnterminatedTagTheEncoderRefuses()
    {
        // Pins the gap the two methods bridge, in both directions. strip() mirrors COA's parser and keeps
        // a malformed tag as feedback; the toolkit's encoder refuses ANY @fk(, by throwing, which for a
        // comment nobody can correct makes the table permanently undiscoverable.
        String stripped = CommentTags.strip("Line total @fk(orders.order_id");
        assertEquals("Line total @fk(orders.order_id", stripped);
        assertThrows(IllegalArgumentException.class,
                () -> dev.coa.connector.constraints.ColumnComment.of(stripped));

        // And neutralise closes it, keeping the prose.
        assertEquals("Line total orders.order_id", CommentTags.neutralise(stripped));
        dev.coa.connector.constraints.ColumnComment.of(CommentTags.neutralise(stripped));
    }

    @Test
    void neutraliseRemovesOnlyTheTagShapedTokenAndKeepsTheProse()
    {
        assertEquals("Line total orders.order_id",
                CommentTags.neutralise("Line total @fk(orders.order_id"));
        assertEquals("before after", CommentTags.neutralise("before @fk( after"));
    }

    @Test
    void neutraliseRepeatsUntilNothingMatches()
    {
        // Removing one token can bring the next into a live position, so one pass is not enough: "@fk( @pk"
        // needs two, and "@fk(@fk(" needs two more.
        assertEquals("", CommentTags.neutralise("@fk( @pk"));
        assertEquals("", CommentTags.neutralise("@fk(@fk("));
        assertEquals("tail", CommentTags.neutralise("@fk(@fk( @pk tail"));
    }

    @Test
    void neutraliseLeavesNearMissesAlone()
    {
        // Same rule as strip: the toolkit accepts these and COA stores them, so removing them would delete
        // a customer's text on both sides' behalf.
        for (String prose : new String[] {
            "value @PK here", "value @pkey here", "value @pk=x here", "value @pk(x) here",
            "value @fk here", "owner bob@pk.example.com", "x@fk(y"}) {
            assertEquals(prose, CommentTags.neutralise(prose), prose);
        }
    }

    @Test
    void neutraliseHandlesNullAndEmpty()
    {
        assertEquals("", CommentTags.neutralise(null));
        assertEquals("", CommentTags.neutralise(""));
        assertEquals("", CommentTags.neutralise("   "));
    }

    @Test
    void stripThenNeutraliseIsAlwaysAcceptableToTheEncoder()
    {
        // The guarantee, over a corpus rather than a case. If the two restated regexes ever drift from the
        // toolkit's guard, this fails.
        String[] corpus = {
            null, "", "   ", "plain prose",
            "@pk", "@fk(a.b)", "@pk @fk(a.b)", "prose @pk more @fk(a.b) end",
            "@fk(orders.order_id", "@fk(", "@fk((", "@fk(@fk(", "@fk( @pk", "@pk @fk(",
            "@fk(\"a)b\".c) then @fk(unterminated",
            "@fk(\"unclosed quote", "@fk(\"\"\")",
            "bob@pk.example.com", "@PK", "@pkey", "@pk=x", "@pk(x)", "@fk", "x@fk(y",
            "tabs\tand\nnewlines @fk( here",
            "@fk(a.b)@pk", "x@pk@fk(", "@@fk(", " @fk(a.b) @fk(c.d) @pk ",
        };
        for (String comment : corpus) {
            String forwarded = CommentTags.neutralise(CommentTags.strip(comment));
            // Throws if the encoder would refuse it, which is the whole point.
            dev.coa.connector.constraints.ColumnComment.of(forwarded);
        }
    }
}
