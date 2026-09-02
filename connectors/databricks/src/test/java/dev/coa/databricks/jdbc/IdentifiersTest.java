// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Identifier quoting, including every name that could otherwise terminate its own quoting. */
class IdentifiersTest
{
    @Test
    void quotesWithBackticks()
    {
        assertEquals("`orders`", Identifiers.quote("orders"));
    }

    @Test
    void doublesAnEmbeddedBacktick()
    {
        // The case the class exists for. A single backtick closes the quoting and turns the rest of the
        // name into SQL.
        assertEquals("```a``b```", Identifiers.quote("`a`b`"));
        assertEquals("`or``ders`", Identifiers.quote("or`ders"));
    }

    @Test
    void leavesADoubleQuoteAlone()
    {
        // A double quote is not the delimiter here, so it needs no escaping, and doubling it would change
        // the name. Databricks accepts it inside a backtick-quoted identifier.
        assertEquals("`say \"hi\"`", Identifiers.quote("say \"hi\""));
    }

    @Test
    void aNameThatWouldEndTheStatementCannotEscape()
    {
        // Not reachable through this connector, since ConnectionConfig refuses a catalog like this and table
        // names come from the source, but the quoting has to hold on its own.
        assertEquals("`orders``; DROP TABLE x; --`",
                Identifiers.quote("orders`; DROP TABLE x; --"));
    }

    @Test
    void refusesANullOrEmptyIdentifier()
    {
        // An empty identifier renders as ``, which is invalid SQL rather than a wrong answer, but it is
        // generated silently and the error names the statement rather than the input.
        assertThrows(IllegalArgumentException.class, () -> Identifiers.quote(null));
        assertThrows(IllegalArgumentException.class, () -> Identifiers.quote(""));
    }

    @Test
    void qualifiesEachSegmentIndependently()
    {
        assertEquals("`main`.`sales`.`orders`", Identifiers.qualify("main", "sales", "orders"));
        assertEquals("`main`.`in``formation`", Identifiers.qualify("main", "in`formation"));
    }

    @Test
    void qualifyRefusesNoSegmentsOrABlankOne()
    {
        assertThrows(IllegalArgumentException.class, Identifiers::qualify);
        assertThrows(IllegalArgumentException.class, () -> Identifiers.qualify("main", ""));
    }
}
