/*
 * Copyright (c) 2016 CUBRID Corporation.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package com.cubrid.plcsql.compiler.ast;

import com.cubrid.jsp.value.DateTimeParser;
import com.cubrid.plcsql.compiler.visitor.AstVisitor;
import java.time.LocalDate;
import java.util.Set;
import org.antlr.v4.runtime.ParserRuleContext;

public class ExprDate extends Expr {

    @Override
    public <R> R accept(AstVisitor<R> visitor) {
        return visitor.visitExprDate(this);
    }

    public final LocalDate date;

    public ExprDate(ParserRuleContext ctx, LocalDate date) {
        super(ctx);

        this.date = date;
    }

    public String javaCode(Set<String> javaTypesUsed) {

        javaTypesUsed.add("java.sql.Date");

        if (date.equals(DateTimeParser.nullDate)) {
            // CAUTION: Return value must be equal to ValueUtility.NULL_DATE
            return "new Date(0 - 1900, 0 - 1, 0)";
        } else {
            return String.format(
                    "new Date(%d - 1900, %d - 1, %d)",
                    date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        }
    }
}