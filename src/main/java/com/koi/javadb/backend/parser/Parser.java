package com.koi.javadb.backend.parser;

import com.koi.javadb.backend.parser.statement.Delete;
import com.koi.javadb.backend.parser.statement.Insert;
import com.koi.javadb.backend.parser.statement.Select;
import com.koi.javadb.backend.parser.statement.Show;
import com.koi.javadb.backend.parser.statement.SingleExpression;
import com.koi.javadb.backend.parser.statement.Update;
import com.koi.javadb.backend.parser.statement.Where;
import com.koi.javadb.common.Errors;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch (token) {
                case "begin":
                    stat = parseBegin();
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        } catch (Exception e) {
            statErr = e;
        }
        try {
            String next = tokenizer.peek();
            if (!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if (statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            return new Show();
        }
        throw Errors.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if (!"set".equals(tokenizer.peek())) {
            throw Errors.InvalidCommandException;
        }
        tokenizer.pop();
        update.fieldName = tokenizer.peek();
        tokenizer.pop();
        if (!"=".equals(tokenizer.peek())) {
            throw Errors.InvalidLogOpException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }
        update.where = parrseWhere();
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();
        if (!"from".equals(tokenizer.peek())) {
            throw Errors.InvalidCommandException;
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Errors.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();
        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();
        if (!"into".equals(tokenizer.peek())) {
            throw Errors.InvalidCommandException;
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Errors.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();
        if (!"values".equals(tokenizer.peek())) {
            throw Errors.InvalidCommandException;
        }
        List<Object> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if ("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);
        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();
        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if ("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field)) {
                    throw Errors.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);
        if (!"from".equals(tokenizer.peek())) {
            throw Errors.InvalidCommandException;
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Errors.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();
        if (!"where".equals(tokenizer.peek())) {
            throw Errors.InvalidCommandException;
        }
        tokenizer.pop();
        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if (!isLogicOp(logicOp)) {
            throw Errors.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if (!"".equals(tokenizer.peek())) {
            throw Errors.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if (!isName(field)) {
            throw Errors.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if (!isCmpOp(op)) {
            throw Errors.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }


    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
