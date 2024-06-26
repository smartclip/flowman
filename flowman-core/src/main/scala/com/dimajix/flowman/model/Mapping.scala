/*
 * Copyright 2019-2022 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.model

import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.DateFormatClass
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.FromUTCTimestamp
import org.apache.spark.sql.catalyst.expressions.FromUnixTime
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IfNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Nvl
import org.apache.spark.sql.catalyst.expressions.Nvl2
import org.apache.spark.sql.catalyst.expressions.String2StringExpression
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.catalyst.expressions.ToUTCTimestamp
import org.apache.spark.sql.catalyst.expressions.ToUnixTimestamp
import org.apache.spark.sql.catalyst.expressions.TruncInstant
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{types => st}
import org.apache.spark.sql.types.StructField
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import com.dimajix.common.IdentityHashSet
import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.documentation.MappingDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.NoSuchMappingOutputException
import com.dimajix.flowman.graph.Column
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.graph.MappingOutput
import com.dimajix.flowman.graph.MappingRef
import com.dimajix.flowman.model
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.SchemaUtils
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.ExpressionParser
import com.dimajix.spark.sql.SchemaUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
import com.dimajix.spark.sql.SchemaUtils.hasExtendedTypeinfo


object Mapping {
    object Properties {
        def apply(context: Context, name: String = "", kind:String = ""): Properties = {
            Properties(
                context,
                Metadata(context, name, Category.MAPPING, kind),
                false,
                false,
                StorageLevel.NONE,
                None
            )
        }
    }
    final case class Properties(
        context:Context,
        metadata:Metadata,
        broadcast:Boolean,
        checkpoint:Boolean,
        cache:StorageLevel,
        documentation:Option[MappingDoc]
    ) extends model.Properties[Properties] {
        require(metadata.category == Category.MAPPING.lower)
        require(metadata.namespace == context.namespace.map(_.name))
        require(metadata.project == context.project.map(_.name))
        require(metadata.version == context.project.flatMap(_.version))

        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))

        def merge(other: Properties): Properties = {
            Properties(
                context,
                metadata.merge(other.metadata),
                broadcast || other.broadcast,
                checkpoint || other.checkpoint,
                if (other.cache != StorageLevel.NONE) other.cache else cache,
                documentation.map(_.merge(other.documentation)).orElse(other.documentation)
            )
        }
        def identifier : MappingIdentifier = MappingIdentifier(name, project.map(_.name))
    }
}


trait Mapping extends Instance {
    override type PropertiesType = Mapping.Properties

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: Category = Category.MAPPING

    /**
      * Returns an identifier for this mapping
      * @return
      */
    def identifier : MappingIdentifier

    /**
     * Returns a (static) documentation of this mapping
     * @return
     */
    def documentation : Option[MappingDoc]

    /**
      * This method should return true, if the resulting dataframe should be broadcast for map-side joins
      * @return
      */
    def broadcast : Boolean

    /**
      * This method should return true, if the resulting dataframe should be checkpointed
      * @return
      */
    def checkpoint : Boolean

    /**
      * Returns the desired storage level. Default should be StorageLevel.NONE
      * @return
      */
    def cache : StorageLevel

    /**
      * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
      * which actually read from physical data.
      * @return
      */
    def requires : Set[ResourceIdentifier]

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      * @return
      */
    def inputs : Set[MappingOutputIdentifier]

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output, which is the default output
     * implicitly used when no output is specified. But eventually, the "main" output is not mandatory, but
     * recommended.
     * @return
     */
    def outputs : Set[String]

    /**
     * Creates an output identifier for the primary output
     * @return
     */
    def output : MappingOutputIdentifier

    /**
     * Creates an output identifier for the specified output name
     * @param name
     * @return
     */
    def output(name:String = "main") : MappingOutputIdentifier

    /**
      * Executes this Mapping and returns a corresponding map of DataFrames per output. The map should contain
      * one entry for each declared output in [[outputs]]. If it contains an additional entry called `cache`, then
      * this [[DataFrame]] will be cached instead of all outputs. The `cache` DataFrame may even well be some
      * internal [[DataFrame]] which is not listed in [[outputs]].
      *
      * @param execution
      * @param input
      * @return
      */
    def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame]

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema. The method should
      * return one entry for each entry declared in [[outputs]].
      * @param input
      * @return
      */
    def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType]

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    def link(linker:Linker) : Unit
}


/**
 * Common base implementation for the MappingType interface
 */
abstract class BaseMapping extends AbstractInstance with Mapping {
    private val logger = LoggerFactory.getLogger(classOf[BaseMapping])
    protected override def instanceProperties : Mapping.Properties

    /**
     * Returns an identifier for this mapping
     * @return
     */
    override def identifier : MappingIdentifier = instanceProperties.identifier

    /**
     * Returns a (static) documentation of this mapping
     * @return
     */
    override def documentation : Option[MappingDoc] =
        instanceProperties.documentation.map(_.copy(mapping=Some(this)))

    /**
     * This method should return true, if the resulting dataframe should be broadcast for map-side joins
     * @return
     */
    override def broadcast : Boolean = instanceProperties.broadcast

    /**
     * This method should return true, if the resulting dataframe should be checkpointed
     * @return
     */
    override def checkpoint : Boolean = instanceProperties.checkpoint

    /**
     * Returns the desired storage level. Default should be StorageLevel.NONE
     * @return
     */
    override def cache : StorageLevel = instanceProperties.cache

    /**
     * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
     * which actually read from physical data.
     * @return
     */
    override def requires : Set[ResourceIdentifier] = Set.empty

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output
     * @return
     */
    override def outputs : Set[String] = Set("main")

    /**
     * Creates an output identifier for the primary output
     * @return
     */
    override def output : MappingOutputIdentifier = {
        MappingOutputIdentifier(identifier, "main")
    }

    /**
     * Creates an output identifier for the specified output name
     * @param name
     * @return
     */
    override def output(name:String = "main") : MappingOutputIdentifier = {
        if (!outputs.contains(name))
            throw new NoSuchMappingOutputException(identifier, name)
        MappingOutputIdentifier(identifier, name)
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. The map might not contain
     * schema information for all outputs, if the schema cannot be inferred.
     * @param input
     * @return
     */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        def extractComment2(expressions: Seq[Expression]) : Option[String] = {
            expressions.flatMap(extractComment).headOption
        }
        def extractComment(expression: Expression) : Option[String] = {
            expression match {
                case n:NamedExpression if n.metadata.contains("comment")=> Some(n.metadata.getString("comment"))
                case c:Cast => extractComment(c.child)
                case a:Alias => extractComment(a.child)
                case c:Coalesce => extractComment2(c.children)
                case a:AggregateExpression => extractComment(a.aggregateFunction)
                case a:AggregateFunction => extractComment2(a.children).map(_ + s" (${a.prettyName.toUpperCase})")
                case s:String2StringExpression => extractComment(s.asInstanceOf[UnaryExpression].child)
                case i:If => extractComment(i.trueValue).orElse(extractComment(i.falseValue))
                case n:IfNull => extractComment(n.left).orElse(extractComment(n.right))
                case n:Nvl => extractComment(n.left).orElse(extractComment(n.right))
                case n:Nvl2 => extractComment(n.expr2).orElse(extractComment(n.expr3))
                case c:CaseWhen if c.branches.size == 1 => extractComment(c.branches.head._2)
                case t:TruncInstant => extractComment(t.right)
                case f:DateFormatClass => extractComment(f.left)
                case u:UnixTimestamp => extractComment(u.left)
                case u:FromUnixTime => extractComment(u.left)
                case t:ToUnixTimestamp => extractComment(t.left)
                case u:FromUTCTimestamp => extractComment(u.left)
                case u:ToUTCTimestamp => extractComment(u.left)
                case _ => None
            }
        }
        def extractType(expression: Expression) : FieldType = {
            expression match {
                case n:NamedExpression if hasExtendedTypeinfo(n.metadata) => Field.of(StructField(n.name, n.dataType, n.nullable, n.metadata)).ftype
                case a:Alias => extractType(a.child)
                case l:Literal if l.dataType == st.StringType =>
                    l.value match {
                        case str:UTF8String => CharType(str.numChars())
                        case _ => FieldType.of(l.dataType)
                    }
                case c:Coalesce => c.children.map(extractType).reduce(SchemaUtils.coerce)
                case c:Concat =>
                    c.children.map(extractType).reduce { (lt,rt) =>
                        (lt,rt) match {
                            case (VarcharType(l), VarcharType(r)) => VarcharType(l + r)
                            case (CharType(l), VarcharType(r)) => VarcharType(l + r)
                            case (VarcharType(l), CharType(r)) => VarcharType(l + r)
                            case (CharType(l), CharType(r)) => CharType(l + r)
                            case _ => FieldType.of(expression.dataType)
                        }
                    }
                case c:Substring =>
                    (extractType(c.str), c.pos, c.len) match {
                        case (VarcharType(l), Literal(pos:Int, _), Literal(len:Int, _)) => VarcharType(math.max(math.min(l-pos+1, len), 1))
                        case (CharType(l), Literal(pos:Int, _), Literal(len:Int, _)) => CharType(math.max(math.min(l-pos+1, len), 1))
                        case _ => FieldType.of(expression.dataType)
                    }
                case s:String2StringExpression => extractType(s.asInstanceOf[UnaryExpression].child)
                case _ => FieldType.of(expression.dataType)
            }
        }
        def extractSchema(df:DataFrame) : StructType = {
            val output = df.queryExecution.analyzed.output
            val expressions = collectExpressions(df.queryExecution.analyzed)
            val attributes = output.map(a => a.name -> expressions.getOrElse(a.exprId, a))
                .map { case (name,expr) =>
                    val field = Field.of(StructField(name, expr.dataType, expr.nullable, expr.metadata))
                    field.copy(
                        ftype = extractType(expr),
                        description = extractComment(expr).orElse(field.description)
                    )
                }

            StructType(attributes)
        }

        // Create dummy data frames
        val replacements = input.map { case (name,schema) =>
            name -> DataFrameBuilder.singleRow(execution.spark, schema.sparkType)
        }

        // Execute mapping
        val results = execute(execution, replacements)

        // Extract schemas
        val schemas = results.map { case (name,df) => name -> extractSchema(df) }

        // Apply documentation
        applyDocumentation(schemas)
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker:Linker) : Unit = {
        val ins = inputs.toSeq.map { in =>
            in -> linker.input(in.mapping, in.output)
        }

        try {
            // Only perform column linking if we really have an input. This will save us from performing a call
            // to execute on sources, which might already be expensive
            if (ins.nonEmpty) {
                linkColumns(linker, ins)
            }
        }
        catch {
            case NonFatal(ex) =>
                logger.warn(s"Cannot infer column lineage for mapping '${identifier}': ${ex.getMessage}")
        }
    }

    private def linkColumns(linker:Linker, ins:Seq[(MappingOutputIdentifier,MappingOutput)]) : Unit = {
        // Create lineage on column level
        val execution = linker.execution

        // Create lineage on column level
        val dummyInputs = ins.map { case(id,in) =>
            val schema = StructType(in.fields.map(_.field))
            // We use the custom NamedAttributes instead of singleRow, since Spark will optimize away important
            // information in UNION operations with Alias(Literal())
            val df = DataFrameBuilder.namedAttributes(execution.spark, schema.sparkType)
            (id,in,df)
        }

        // Execute mapping
        val replacements = dummyInputs.map { case(id,_,df) => id -> df }.toMap
        val results = execute(execution, replacements)

        // Lookup source columns
        val inputColumns = dummyInputs.flatMap { case(id,out,df) =>
            val expressions = df.queryExecution.analyzed.expressions
            expressions.collect { case e:NamedExpression => e.exprId -> out }
        }.toMap

        def lookupSourceColumns(expression: Expression) : Seq[Column] = {
            expression match {
                case n:NamedExpression => lookupColumn(n.name, n.exprId).toSeq ++ n.children.flatMap(lookupSourceColumns)
                case e => e.children.flatMap(lookupSourceColumns)
            }
        }
        def lookupColumn(name:String, exprId:ExprId) : Option[Column] = {
            inputColumns.get(exprId).flatMap { out =>
                // TODO: Support nested fields
                out.fields.find(_.name == name)
            }
        }

        val self = linker.node.asInstanceOf[MappingRef]
        // For each generated mapping output
        results.foreach { case(name,df) =>
            // find an appropriate  MappingOutput in the graph
            self.outputs.find(_.name == name).foreach { out =>
                val attributes = MapIgnoreCase(resolveAttributes(df).map(a => a.name -> a))
                // For each field in the MappingOutput of the graph
                out.fields.foreach { col =>
                    // TODO: Support nested fields
                    // Find the attribute of the transformed DataFrame
                    attributes.get(col.name) match {
                        case Some(att) =>
                            // Use an IdentityHashset for deduplication
                            IdentityHashSet(lookupSourceColumns(att):_*).foreach(src => linker.connect(src, col))
                        case None =>
                        // Should not happen
                    }
                }
            }
        }
    }
    private def resolveAttributes(df:DataFrame) : Seq[NamedExpression] = {
        resolveAttributes(df.queryExecution.analyzed)
    }
    private def resolveAttributes(plan:LogicalPlan) : Seq[NamedExpression] = {
        val expressions = collectExpressions(plan)
        val output = plan.output
        output.map(a => expressions.getOrElse(a.exprId, a))
    }
    private def collectExpressions(plan:LogicalPlan) : Map[ExprId, NamedExpression] = {
        plan match {
            // Special handling for UNIONs, which otherwise would only collect columns of first UNION child
            case union:Union =>
                val expressions = union.output
                val childExpressions = union.children.map(resolveAttributes)
                // The Alias(Coalesce()) expression is a workaround to collect all columns of all children into
                // a single expression, which also needs to be a NamedExpression
                expressions.zipWithIndex.map { case(e,i) => e.exprId -> Alias(Coalesce(childExpressions.map(_(i))),e.name)() }.toMap
            case plan:LogicalPlan =>
                val expressions = plan.expressions.collect { case n:NamedExpression => n.exprId -> n }.toMap
                val childExpressions = plan.children.flatMap(collectExpressions)
                expressions ++ childExpressions
        }
    }

    /**
     * Applies optional documentation to the result of a [[describe]]
     * @param schemas
     * @return
     */
    protected def applyDocumentation(schemas:Map[String,StructType]) : Map[String,StructType] = {
        val outputDoc = documentation.map(_.outputs.map(o => o.identifier.output -> o).toMap).getOrElse(Map())
        schemas.map { case (output,schema) =>
            output -> outputDoc.get(output)
                .flatMap(_.schema.map(_.enrich(schema)))
                .getOrElse(schema)
        }
    }

    protected def applyDocumentation(output:String, schema:StructType) : StructType = {
        documentation.flatMap(_.outputs.find(_.identifier.output == output))
            .flatMap(_.schema.map(_.enrich(schema)))
            .getOrElse(schema)
    }

    protected def applyFilter(df:DataFrame, filter:Option[String], inputs:Map[MappingOutputIdentifier, DataFrame]) : DataFrame = {
        filter match {
            case Some(filter) =>
                val deps = ExpressionParser.resolveDependencies(filter)
                    .map(d => d -> inputs(MappingOutputIdentifier(d)))
                DataFrameUtils.withTempViews(deps) {
                    df.where(expr(filter))
                }
            case None => df
        }
    }

    protected def expressionDependencies(expression:Option[String]) : Set[MappingOutputIdentifier] = {
        expression match {
            case None => Set.empty
            case Some(e) => expressionDependencies(e)
        }
    }
    protected def expressionDependencies(expression:String) : Set[MappingOutputIdentifier] = {
        ExpressionParser.resolveDependencies(expression).map(MappingOutputIdentifier.apply)
    }
}
