/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.orc;

import java.nio.ByteBuffer;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.Predicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf.Type;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgument.Builder;
import org.apache.orc.storage.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;

public class OrcFilters {

  private OrcFilters() {
  }

  public static SearchArgument convert(Schema schema, Expression expr, boolean caseSensitive) {
    ConvertFilterToOrc toOrc =
        new ConvertFilterToOrc(schema, caseSensitive, SearchArgumentFactory.newBuilder());
    SearchArgument.Builder builder = visit(expr, toOrc);
    return builder.build();
  }

  public static SearchArgument.Builder visit(Expression expr, ConvertFilterToOrc visitor) {
    if (expr instanceof Predicate) {
      if (expr instanceof BoundPredicate) {
        return visitor.predicate((BoundPredicate<?>) expr);
      } else {
        return visitor.predicate((UnboundPredicate<?>) expr);
      }
    } else {
      switch (expr.op()) {
        case TRUE:
          return visitor.alwaysTrue();
        case FALSE:
          return visitor.alwaysFalse();
        case NOT:
          Not not = (Not) expr;
          visitor.getBuilder().startNot();
          visitor.not(visit(not.child(), visitor));
          return visitor.getBuilder().end();
        case AND:
          And and = (And) expr;
          visitor.getBuilder().startAnd();
          visitor.and(visit(and.left(), visitor), visit(and.right(), visitor));
          return visitor.getBuilder().end();
        case OR:
          Or or = (Or) expr;
          visitor.getBuilder().startOr();
          visitor.or(visit(or.left(), visitor), visit(or.right(), visitor));
          return visitor.getBuilder().end();
        default:
          throw new UnsupportedOperationException("Unknown operation: " + expr.op());
      }
    }
  }

  private static class ConvertFilterToOrc extends ExpressionVisitor<SearchArgument.Builder> {
    private final Schema schema;
    private final boolean caseSensitive;
    private final SearchArgument.Builder builder;

    private ConvertFilterToOrc(
        Schema schema, boolean caseSensitive, SearchArgument.Builder builder) {
      this.schema = schema;
      this.caseSensitive = caseSensitive;
      this.builder = builder;
    }

    @SuppressWarnings("unchecked")
    private static <C extends Comparable<C>> C getOrcPrimitive(Literal<?> lit) {
      if (lit == null) {
        return null;
      }

      // TODO: this needs to convert to handle BigDecimal and UUID
      Object value = lit.value();
      if (value instanceof Number) {
        return (C) lit.value();
      } else if (value instanceof CharSequence) {
        return (C) value.toString();
      } else if (value instanceof ByteBuffer) {
        return (C) new String(((ByteBuffer) value).array());
      }
      throw new UnsupportedOperationException(
          "Type not supported yet: " + value.getClass().getName());
    }

    public SearchArgument.Builder getBuilder() {
      return builder;
    }

    @Override
    public Builder alwaysTrue() {
      return builder.literal(TruthValue.YES);
    }

    @Override
    public Builder alwaysFalse() {
      return builder.literal(TruthValue.NO);
    }

    @Override
    public <T> Builder predicate(BoundPredicate<T> pred) {
      if (!(pred.term() instanceof BoundReference)) {
        throw new UnsupportedOperationException(
            "Cannot convert non-reference to Orc filter: " + pred.term());
      }
      Operation op = pred.op();
      BoundReference<T> ref = (BoundReference<T>) pred.term();
      String path = schema.idToAlias(ref.fieldId());
      Literal<T> lit;
      if (pred.isUnaryPredicate()) {
        lit = null;
      } else if (pred.isLiteralPredicate()) {
        lit = pred.asLiteralPredicate().literal();
      } else {
        throw new UnsupportedOperationException("Cannot convert to Orc filter: " + pred);
      }

      switch (ref.type().typeId()) {
        case BOOLEAN:
          return pred(op, path, Type.BOOLEAN, getOrcPrimitive(lit));
        case DATE:
          return pred(op, path, Type.DATE, getOrcPrimitive(lit));
        case INTEGER:
        case LONG:
          return pred(op, path, Type.LONG, getOrcPrimitive(lit));
        case TIME:
        case TIMESTAMP:
          return pred(op, path, Type.TIMESTAMP, getOrcPrimitive(lit));
        case FLOAT:
        case DOUBLE:
          return pred(op, path, Type.FLOAT, getOrcPrimitive(lit));
        case STRING:
        case UUID:
        case FIXED:
        case BINARY:
          return pred(op, path, Type.STRING, getOrcPrimitive(lit));
        case DECIMAL:
          return pred(op, path, Type.DECIMAL, getOrcPrimitive(lit));
      }
      throw new UnsupportedOperationException("Cannot convert to Orc filter: " + pred);
    }

    @Override
    public <T> Builder predicate(UnboundPredicate<T> pred) {
      Expression bound = bind(pred);
      if (bound instanceof BoundPredicate) {
        return predicate((BoundPredicate<?>) bound);
      } else if (bound == Expressions.alwaysTrue()) {
        return builder.literal(TruthValue.YES);
      } else if (bound == Expressions.alwaysFalse()) {
        return builder.literal(TruthValue.NO);
      }
      throw new UnsupportedOperationException("Cannot convert to Parquet filter: " + pred);
    }

    private SearchArgument.Builder pred(
        Operation op, String column, PredicateLeaf.Type type, Object literal) {
      switch (op) {
        case IS_NULL:
          return builder.isNull(column, type);
        case NOT_NULL:
          return builder.startNot().isNull(column, type).end();
        case EQ:
          return builder.equals(column, type, literal);
        case NOT_EQ:
          return builder.startNot().equals(column, type, literal).end();
        case GT:
          return builder.startNot().lessThanEquals(column, type, literal).end();
        case GT_EQ:
          return builder.startNot().lessThan(column, type, literal).end();
        case LT:
          return builder.lessThan(column, type, literal);
        case LT_EQ:
          return builder.lessThanEquals(column, type, literal);
        default:
          throw new UnsupportedOperationException("Unsupported predicate operation: " + op);
      }
    }

    private Expression bind(UnboundPredicate<?> pred) {
      return pred.bind(schema.asStruct(), caseSensitive);
    }
  }
}
