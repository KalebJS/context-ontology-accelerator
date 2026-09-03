// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package dev.coa.databricks.metadata;

import dev.coa.connector.metadata.CoaTable;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Schema assembly: declared keys becoming comment tags, and those comments landing where Athena reads
 * them.
 *
 * <p>The Arrow-placement assertion is the important one. Athena reads column comments from the
 * <b>schema's</b> metadata map keyed by column name and ignores metadata on an Arrow field, and getting
 * it wrong fails silently: {@code SELECT} works, {@code DESCRIBE} returns no comment column, and no tag
 * reaches COA.
 */
class TableAssemblerTest
{
    private static final ArrowType BIGINT = Types.MinorType.BIGINT.getType();
    private static final ArrowType VARCHAR = Types.MinorType.VARCHAR.getType();

    private static ColumnDefinition column(String name, String type, String comment)
    {
        return new ColumnDefinition(name, type, comment);
    }

    @Test
    void assemblesColumnsTypesAndProse()
    {
        CoaTable table = TableAssembler.assemble("orders", Arrays.asList(
                column("order_id", "bigint", "Surrogate key for the order"),
                column("email", "string", null)),
                DeclaredKeys.none());

        assertEquals(Arrays.asList("order_id", "email"), table.columnNames());
        Map<String, String> comments = table.toTableSchema().toArrowSchema().getCustomMetadata();
        assertEquals("Surrogate key for the order", comments.get("order_id"));
        assertNull(comments.get("email"));
    }

    @Test
    void commentsGoInTheSchemasMetadataAndNotOnAnyField()
    {
        CoaTable table = TableAssembler.assemble("orders", Collections.singletonList(
                column("order_id", "bigint", "Surrogate key")),
                DeclaredKeys.builder().primaryKeyColumn("order_id").build());

        org.apache.arrow.vector.types.pojo.Schema arrow = table.toTableSchema().toArrowSchema();
        assertEquals("Surrogate key @pk", arrow.getCustomMetadata().get("order_id"));
        for (Field field : arrow.getFields()) {
            assertTrue(field.getMetadata().isEmpty(),
                    field.getName() + " carries field metadata, which Athena delivers and ignores");
        }
    }

    @Test
    void aCompositePrimaryKeyTagsEveryMemberColumnInColumnOrder()
    {
        // The grammar has no ordinal: COA reads a composite primary key as the set of @pk columns in
        // DESCRIBE order, which is declaration order here.
        CoaTable table = TableAssembler.assemble("orders", Arrays.asList(
                column("region_code", "string", "Region code"),
                column("order_num", "bigint", "Order number"),
                column("order_total", "decimal(10,2)", "Total")),
                DeclaredKeys.builder()
                        .primaryKeyColumn("region_code")
                        .primaryKeyColumn("order_num")
                        .build());

        Map<String, String> comments = table.toTableSchema().toArrowSchema().getCustomMetadata();
        assertEquals("Region code @pk", comments.get("region_code"));
        assertEquals("Order number @pk", comments.get("order_num"));
        assertEquals("Total", comments.get("order_total"));
        assertEquals(Arrays.asList("region_code", "order_num", "order_total"), table.columnNames());
    }

    @Test
    void aCompositeForeignKeyIsOneTagPerChildColumnNamingItsOwnParent()
    {
        // The format's subtlest case, and the one the naive constraint join gets wrong. The child columns
        // are named differently from their parents, in an order where alphabetical sorting would pair them
        // the other way round.
        CoaTable table = TableAssembler.assemble("order_lines", Arrays.asList(
                column("line_id", "bigint", "Line surrogate key"),
                column("order_region", "string", "Parent region"),
                column("order_id", "bigint", "Parent order number")),
                DeclaredKeys.builder()
                        .primaryKeyColumn("line_id")
                        .foreignKey("order_region", "orders", "region_code")
                        .foreignKey("order_id", "orders", "order_num")
                        .build());

        Map<String, String> comments = table.toTableSchema().toArrowSchema().getCustomMetadata();
        assertEquals("Line surrogate key @pk", comments.get("line_id"));
        assertEquals("Parent region @fk(orders.region_code)", comments.get("order_region"));
        assertEquals("Parent order number @fk(orders.order_num)", comments.get("order_id"));
        // Never one tag listing both: COA stores a composite foreign key as N single-column records.
        assertTrue(!comments.get("order_region").contains("order_num"),
                comments.get("order_region"));
    }

    @Test
    void aColumnCanBeBothAPrimaryKeyMemberAndAForeignKey()
    {
        CoaTable table = TableAssembler.assemble("order_lines", Collections.singletonList(
                column("order_id", "bigint", "Order this line belongs to")),
                DeclaredKeys.builder()
                        .primaryKeyColumn("order_id")
                        .foreignKey("order_id", "orders", "order_id")
                        .build());

        assertEquals("Order this line belongs to @pk @fk(orders.order_id)",
                table.toTableSchema().toArrowSchema().getCustomMetadata().get("order_id"));
    }

    @Test
    void aCustomerAuthoredTagIsStrippedBeforeTheConnectorsOwnIsAdded()
    {
        // The channel must carry only what the connector put there. A tag written in Databricks is
        // byte-identical to a generated one by the time COA sees it.
        CoaTable table = TableAssembler.assemble("order_lines", Collections.singletonList(
                column("sku", "string", "Contact bob@pk.example.com about this @pk column")),
                DeclaredKeys.none());

        // The email survives, by the left-boundary rule, and the hand-written @pk does not.
        assertEquals("Contact bob@pk.example.com about this column",
                table.toTableSchema().toArrowSchema().getCustomMetadata().get("sku"));
    }

    @Test
    void aCustomerAuthoredForeignKeyCannotMintARelationship()
    {
        CoaTable table = TableAssembler.assemble("order_lines", Collections.singletonList(
                column("sku", "string", "Stock unit @fk(payroll.ssn)")),
                DeclaredKeys.none());

        assertEquals("Stock unit",
                table.toTableSchema().toArrowSchema().getCustomMetadata().get("sku"));
    }

    @Test
    void aMalformedCustomerTagDoesNotMakeTheTableUndiscoverable()
    {
        // The regression that matters most here. A Unity Catalog comment of
        // 'Line total @fk(orders.order_id' is enough: strip() keeps it, mirroring COA's parser, and the
        // toolkit's encoder refuses any @fk( by throwing, which is not an SQLException and so is not
        // classified. DESCRIBE then fails permanently for the table and takes the schema's scan with it.
        CoaTable table = TableAssembler.assemble("order_lines", Collections.singletonList(
                column("note", "string", "Line total @fk(orders.order_id")),
                DeclaredKeys.none());

        assertEquals("Line total orders.order_id",
                table.toTableSchema().toArrowSchema().getCustomMetadata().get("note"));
    }

    @Test
    void everyTagShapedCustomerCommentAssemblesWithoutThrowing()
    {
        // A whole table of the shapes a data engineer can write, assembled in one go: the failure guarded
        // against takes out the entire table rather than one column.
        List<ColumnDefinition> columns = Arrays.asList(
                column("unterminated_fk", "string", "Line total @fk(orders.order_id"),
                column("double_open_fk", "string", "Two of them @fk(@fk("),
                column("fk_then_pk", "string", "Bracket then a key @fk( @pk"),
                column("pk_near_miss", "string",
                        "Near misses: @PK @pkey @pk=x @pk(x) and bob@pk.example.com"),
                column("bare_fk", "string", "No bracket so no tag: @fk"),
                column("unclosed_quote", "string", "@fk(\"never closes"));

        CoaTable table = TableAssembler.assemble("malformed_comments", columns, DeclaredKeys.none());
        Map<String, String> comments = table.toTableSchema().toArrowSchema().getCustomMetadata();

        // The near misses survive verbatim, because COA stores them too.
        assertEquals("Near misses: @PK @pkey @pk=x @pk(x) and bob@pk.example.com",
                comments.get("pk_near_miss"));
        assertEquals("No bracket so no tag: @fk", comments.get("bare_fk"));
        // And nothing tag-shaped the encoder would refuse survives.
        for (String comment : comments.values()) {
            assertTrue(!comment.contains("@fk("), comment);
        }
    }

    @Test
    void aColumnNamesCaseIsPreserved()
    {
        // information_schema lower-cases table names but preserves column names, and the preserved
        // spelling has to match the field names Athena projects.
        CoaTable table = TableAssembler.assemble("orders", Collections.singletonList(
                column("CustomerName", "string", "Mixed-case column name on purpose")),
                DeclaredKeys.none());
        assertEquals(Collections.singletonList("CustomerName"), table.columnNames());
    }

    @Test
    void quotesAParentNameThatNeedsIt()
    {
        CoaTable table = TableAssembler.assemble("order_lines", Collections.singletonList(
                column("order_id", "bigint", null)),
                DeclaredKeys.builder()
                        .foreignKey("order_id", "my orders", "order id")
                        .build());
        assertEquals("@fk(\"my orders\".\"order id\")",
                table.toTableSchema().toArrowSchema().getCustomMetadata().get("order_id"));
    }

    @Test
    void aTableWithNoColumnsIsAnError()
    {
        // Empty means the table does not exist in this catalog and schema, or the principal cannot see it.
        // Returning an empty schema would present as a table with no columns.
        List<ColumnDefinition> none = Collections.emptyList();
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> TableAssembler.assemble("ghost", none, DeclaredKeys.none()));
        assertTrue(failure.getMessage().contains("ghost"), failure.getMessage());
    }

    @Test
    void nullKeysAreTreatedAsNoKeys()
    {
        CoaTable table = TableAssembler.assemble("orders", Collections.singletonList(
                column("order_id", "bigint", "key")), null);
        assertEquals("key", table.toTableSchema().toArrowSchema().getCustomMetadata().get("order_id"));
    }

    @Test
    void columnTypesReachTheArrowSchema()
    {
        CoaTable table = TableAssembler.assemble("orders", Arrays.asList(
                column("order_id", "bigint", null),
                column("email", "string", null)),
                DeclaredKeys.none());
        List<Field> fields = table.toTableSchema().toArrowSchema().getFields();
        assertEquals(BIGINT, fields.get(0).getType());
        assertEquals(VARCHAR, fields.get(1).getType());
    }
}
