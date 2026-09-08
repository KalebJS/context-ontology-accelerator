// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.metadata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Removes any {@code @pk} or {@code @fk(...)} tag a customer wrote into a Unity Catalog column
 * comment, so the tag channel carries only what this connector put in it.
 *
 * <p>COA recovers declared keys from column comments as {@code @pk} and {@code @fk(table.column)}
 * tags, and a Unity Catalog column comment is set with {@code COMMENT ON COLUMN}, which anyone holding
 * {@code MODIFY} can run. So without this class a comment reading {@code "customer surrogate key @pk"}
 * mints a primary key Unity Catalog never declared, and {@code "@fk(payroll.ssn)"} asserts a
 * relationship into a table that may not exist. COA's parser cannot tell a hand-written tag from a
 * generated one; the connector is the last place the distinction exists, because here the keys come
 * from {@code table_constraints} and the comment from {@code columns}.
 *
 * <p>Two of COA's parser rules are restated here so the two sides agree. A tag must not follow an
 * identifier character, so {@code owner bob@pk.example.com} is prose and survives. And {@code @fk(}'s
 * operand ends at the first {@code )} outside a double-quoted segment, so {@code @fk("a)b".c)} is one
 * tag and the prose after it survives.
 *
 * <p>Near misses are left alone because COA leaves them alone: {@code @PK} (case is significant),
 * {@code @pkey}, {@code @pk=x} and {@code @pk(x)} are prose on both sides, and removing them here would
 * delete text a customer wrote and COA would have kept. An unterminated {@code @fk(} is also left
 * alone: COA reports it and keeps it in the description, and that surviving text is the only feedback
 * its author gets.
 *
 * <p>Which means {@link #strip(String)} alone is not enough to forward a comment. It can return text
 * still containing {@code @fk(}, and the toolkit's encoder refuses any {@code @fk(}, closed or not, by
 * throwing. A caller forwarding a comment has to run {@link #neutralise(String)} after {@code strip}.
 * {@code TableAssembler} does.
 */
public final class CommentTags
{
    /**
     * A live {@code @pk}: exact case, no identifier character in front, and not followed by {@code =} or
     * {@code (}, which are near misses COA reports and leaves alone.
     */
    private static final Pattern LIVE_PK =
            Pattern.compile("(?<![A-Za-z0-9_$])@pk(?![A-Za-z0-9_=(])");

    /** A live {@code @fk(}. The bracket is what makes COA read an operand. */
    private static final Pattern LIVE_FK_OPEN =
            Pattern.compile("(?<![A-Za-z0-9_$])@fk\\(");

    private CommentTags()
    {
    }

    /**
     * A column comment with every live tag removed and whitespace collapsed. Null in, {@code ""} out.
     */
    public static String strip(String comment)
    {
        if (comment == null || comment.isEmpty()) {
            return "";
        }
        String withoutForeignKeys = stripForeignKeys(comment);
        String withoutTags = LIVE_PK.matcher(withoutForeignKeys).replaceAll(" ");
        return collapseWhitespace(withoutTags);
    }

    /**
     * Removes any remaining tag-shaped sequence the toolkit's comment encoder would refuse, leaving the
     * prose around it. Null in, {@code ""} out.
     *
     * <p>Separate from {@link #strip(String)} because the two answer to different owners. {@code strip}
     * mirrors COA's parser, which leaves a malformed {@code @fk(} in place as feedback to its author.
     * The toolkit's encoder is broader: it refuses any {@code @fk(} in prose, closed or not, to stop a
     * connector author hand-writing a tag instead of calling {@code foreignKey(...)}. That guard is
     * right for its own callers and wrong here, where the prose is a customer's column comment that COA
     * neither controls nor can ask to have corrected. It refuses by throwing
     * {@link IllegalArgumentException}, which is not a {@link java.sql.SQLException} and so is not
     * classified on the way out, so one comment reading {@code 'Line total @fk(orders.order_id'} fails
     * that table's {@code DESCRIBE} permanently and takes the schema's whole scan with it.
     *
     * <p>Done by pattern rather than by catching the encoder's exception: a handler would discard the
     * whole comment on a fault it cannot describe, and would stop working silently if the toolkit ever
     * refused for a second reason.
     *
     * <p>Only the tag-shaped token goes: the four characters {@code @fk(}, or a live {@code @pk}. The
     * prose around it becomes the column's description in the ontology. Removal repeats until nothing
     * matches, since removing one token can bring the next into a live position ({@code "@fk( @pk"}
     * needs two passes). The text shrinks each pass, so it terminates. Near misses are left alone, as
     * {@code strip} leaves them.
     *
     * @param text prose, normally the output of {@link #strip(String)}.
     */
    public static String neutralise(String text)
    {
        if (text == null || text.isEmpty()) {
            return "";
        }
        String working = text;
        while (true) {
            // The foreign-key form first: it is the one strip() can leave behind, and removing it can
            // expose a @pk that was inside the operand.
            String next = removeFirstMatch(LIVE_FK_OPEN, working);
            if (next == null) {
                next = removeFirstMatch(LIVE_PK, working);
            }
            if (next == null) {
                return collapseWhitespace(working);
            }
            working = next;
        }
    }

    /**
     * {@code text} with the first match replaced by a space, so two words cannot be glued together;
     * null when the pattern does not match, which is the loop's exit.
     */
    private static String removeFirstMatch(Pattern pattern, String text)
    {
        Matcher matcher = pattern.matcher(text);
        if (!matcher.find()) {
            return null;
        }
        return text.substring(0, matcher.start()) + ' ' + text.substring(matcher.end());
    }

    /**
     * Whether {@link #strip(String)} would remove anything. For logging that it happened; the strip
     * itself is unconditional.
     */
    public static boolean carriesTag(String comment)
    {
        if (comment == null || comment.isEmpty()) {
            return false;
        }
        return LIVE_PK.matcher(comment).find() || closingBracket(comment) >= 0;
    }

    /** Removes every {@code @fk(...)} whose bracket closes. Left to right, one pass. */
    private static String stripForeignKeys(String comment)
    {
        StringBuilder out = new StringBuilder(comment.length());
        String remaining = comment;
        while (true) {
            Matcher open = LIVE_FK_OPEN.matcher(remaining);
            if (!open.find()) {
                out.append(remaining);
                return out.toString();
            }
            int operandStart = open.end();
            int close = findClosingBracket(remaining, operandStart);
            if (close < 0) {
                // Unterminated. COA reports and keeps it, so keep it, and stop: as far as COA is
                // concerned everything after an unclosed bracket is inside the operand.
                out.append(remaining);
                return out.toString();
            }
            out.append(remaining, 0, open.start()).append(' ');
            remaining = remaining.substring(close + 1);
        }
    }

    /** The index of the first live {@code @fk(}'s closing bracket, or -1 if there is none. */
    private static int closingBracket(String comment)
    {
        Matcher open = LIVE_FK_OPEN.matcher(comment);
        if (!open.find()) {
            return -1;
        }
        return findClosingBracket(comment, open.end());
    }

    /**
     * Scans for the {@code )} that ends an operand, honouring the grammar's per-segment double quoting:
     * inside a quoted segment a {@code )} is data, and {@code ""} is an escaped quote.
     *
     * @param from the index just past {@code @fk(}.
     * @return the closing bracket's index, or -1 when it never closes.
     */
    private static int findClosingBracket(String text, int from)
    {
        boolean inQuotes = false;
        for (int i = from; i < text.length(); i++) {
            char character = text.charAt(i);
            if (character == '"') {
                // A doubled quote inside a quoted segment is a literal quote, not the end of it.
                if (inQuotes && i + 1 < text.length() && text.charAt(i + 1) == '"') {
                    i++;
                    continue;
                }
                inQuotes = !inQuotes;
                continue;
            }
            if (character == ')' && !inQuotes) {
                return i;
            }
        }
        return -1;
    }

    private static String collapseWhitespace(String text)
    {
        StringBuilder out = new StringBuilder(text.length());
        boolean pendingSpace = false;
        for (int i = 0; i < text.length(); i++) {
            char character = text.charAt(i);
            if (Character.isWhitespace(character)) {
                pendingSpace = out.length() > 0;
                continue;
            }
            if (pendingSpace) {
                out.append(' ');
                pendingSpace = false;
            }
            out.append(character);
        }
        return out.toString();
    }
}
