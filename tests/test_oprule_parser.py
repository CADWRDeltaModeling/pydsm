"""
Unit tests for pydsm.analysis.oprule_parser.

Covers:
  - Lexer (tokenization of all token types)
  - Parser (AST structure for real integration-study expressions)
  - Evaluator (numeric / boolean / Timestamp results against mock data)
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from pydsm.analysis.oprule_parser import (
    # Lexer
    tokenize, Token, T_NUMBER, T_NAME, T_KEYWORD, T_REFDATE, T_REFTIME, T_OP, T_EOF,
    # AST nodes
    Num, BoolLit, DateTimeLit, DateTimeNow,
    BinOp, CmpOp, LogOp, UnaryOp, FuncCall, BareRef, ActionSpec,
    # Entry points
    parse_trigger, parse_expr, parse_action, eval_node,
    # Errors
    UnsupportedFeatureError, ParseError,
    # Hydro dependency walker
    needs_hydro,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _time_idx(start="2015-01-01", periods=24, freq="1h"):
    return pd.date_range(start, periods=periods, freq=freq)


# ---------------------------------------------------------------------------
# Lexer tests
# ---------------------------------------------------------------------------


class TestLexer:

    def _types(self, text):
        return [(t.type, t.value) for t in tokenize(text) if t.type != T_EOF]

    def test_number_integer(self):
        toks = tokenize("42")
        assert toks[0].type == T_NUMBER
        assert toks[0].value == 42.0

    def test_number_float(self):
        toks = tokenize("3.14")
        assert toks[0].type == T_NUMBER
        assert abs(toks[0].value - 3.14) < 1e-10

    def test_number_scientific(self):
        toks = tokenize("1.5e-3")
        assert toks[0].type == T_NUMBER
        assert abs(toks[0].value - 0.0015) < 1e-15

    def test_refdate_uppercase(self):
        toks = tokenize("20SEP2018")
        assert toks[0].type == T_REFDATE
        assert toks[0].value == "20SEP2018"

    def test_refdate_lowercase(self):
        toks = tokenize("1jan2020")
        assert toks[0].type == T_REFDATE

    def test_refdate_not_split(self):
        # "20SEP2018" must not be tokenised as NUMBER + NAME + NUMBER
        types = [t.type for t in tokenize("20SEP2018") if t.type != T_EOF]
        assert types == [T_REFDATE]

    def test_reftime_colon_not_split(self):
        # "12:00" must arrive as a single REFTIME token, not NUMBER:NUMBER
        types = [t.type for t in tokenize("12:00") if t.type != T_EOF]
        assert types == [T_REFTIME]
        assert tokenize("12:00")[0].value == "12:00"

    def test_month_as_number(self):
        for mon, expected in [("JAN", 1.0), ("MAR", 3.0), ("DEC", 12.0)]:
            toks = tokenize(mon)
            assert toks[0].type == T_NUMBER, f"{mon} should be NUMBER"
            assert toks[0].value == expected

    def test_keyword_and(self):
        toks = tokenize("AND")
        assert toks[0].type == T_KEYWORD
        assert toks[0].value == "AND"

    def test_keyword_min2_not_split(self):
        # MIN2 must be KEYWORD, not NAME or confused with MIN
        toks = tokenize("MIN2")
        assert toks[0].type == T_KEYWORD
        assert toks[0].value == "MIN2"

    def test_name(self):
        toks = tokenize("dcc_op")
        assert toks[0].type == T_NAME
        assert toks[0].value == "dcc_op"

    def test_name_with_at(self):
        toks = tokenize("old_r@tracy")
        assert toks[0].type == T_NAME

    def test_multichar_op_ge(self):
        toks = tokenize(">=")
        assert toks[0].type == T_OP
        assert toks[0].value == ">="

    def test_multichar_op_ne(self):
        toks = tokenize("<>")
        assert toks[0].type == T_OP
        assert toks[0].value == "<>"

    def test_multichar_op_eq(self):
        toks = tokenize("==")
        assert toks[0].type == T_OP
        assert toks[0].value == "=="

    def test_define_vs_eq(self):
        # Single "=" is DEFINE, "==" is CMP_EQ
        toks = tokenize("= ==")
        assert toks[0].value == "="
        assert toks[1].value == "=="

    def test_refseason_raises_warning(self):
        # REFSEASON should emit a warning and produce a sentinel
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            toks = tokenize("28MAY")
        # Warning should have been logged (via logger); the token should be _REFSEASON_
        sentinel_toks = [t for t in toks if t.value == "_REFSEASON_"]
        assert len(sentinel_toks) == 1

    def test_whitespace_skipped(self):
        toks = [t for t in tokenize("  1.0  +  2.0  ") if t.type != T_EOF]
        assert len(toks) == 3  # NUMBER, OP, NUMBER

    def test_ts_arglist_tokens(self):
        types = self._types("ts(name=fb_install)")
        # T_NAME(ts), T_OP('('), T_NAME(name), T_OP('='), T_NAME(fb_install), T_OP(')')
        assert types[0] == (T_NAME, "ts")
        assert types[1] == (T_OP, "(")
        assert types[2] == (T_NAME, "name")
        assert types[3] == (T_OP, "=")
        assert types[4] == (T_NAME, "fb_install")
        assert types[5] == (T_OP, ")")


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------


class TestParser:

    # ---- simple literals ---

    def test_true(self):
        assert parse_trigger("TRUE") == BoolLit(True)

    def test_false(self):
        assert parse_trigger("FALSE") == BoolLit(False)

    def test_number_literal(self):
        assert parse_expr("3.5") == Num(3.5)

    def test_datetime_keyword(self):
        assert parse_trigger("DATETIME >= 20SEP2018") == CmpOp(
            ">=", DateTimeNow(), DateTimeLit(pd.Timestamp("2018-09-20"))
        )

    def test_datetime_with_colon_time(self):
        ast = parse_trigger("DATETIME >= 20SEP2018 12:00")
        assert isinstance(ast, CmpOp)
        assert isinstance(ast.right, DateTimeLit)
        assert ast.right.value == pd.Timestamp("2018-09-20 12:00")

    def test_datetime_with_hhmm_time(self):
        ast = parse_trigger("DATETIME >= 20SEP2018 1200")
        assert isinstance(ast, CmpOp)
        assert ast.right.value == pd.Timestamp("2018-09-20 12:00")

    # ---- arithmetic precedence ---

    def test_add_mul_precedence(self):
        # 1.5 + 2.0 * 3.0  →  1.5 + (2.0 * 3.0)
        ast = parse_expr("1.5 + 2.0 * 3.0")
        assert isinstance(ast, BinOp) and ast.op == "+"
        assert ast.left == Num(1.5)
        assert isinstance(ast.right, BinOp) and ast.right.op == "*"

    def test_power_right_assoc(self):
        # 2^3^4 should be 2^(3^4)
        ast = parse_expr("2^3^4")
        assert isinstance(ast, BinOp) and ast.op == "^"
        assert ast.left == Num(2.0)
        assert isinstance(ast.right, BinOp) and ast.right.op == "^"

    def test_unary_neg(self):
        ast = parse_expr("-1")
        assert ast == UnaryOp("NEG", Num(1.0))

    def test_unary_neg_times_ts(self):
        # -1*ts(name=dicu_div_151_flow)
        ast = parse_expr("-1*ts(name=dicu_div_151_flow)")
        assert isinstance(ast, BinOp) and ast.op == "*"
        assert ast.left == UnaryOp("NEG", Num(1.0))
        assert isinstance(ast.right, FuncCall) and ast.right.name == "ts"
        assert ast.right.kwargs["name"] == "dicu_div_151_flow"

    # ---- ts() arglist ---

    def test_ts_name_kwarg(self):
        ast = parse_expr("ts(name=fb_install)")
        assert isinstance(ast, FuncCall)
        assert ast.name == "ts"
        assert ast.kwargs["name"] == "fb_install"

    def test_ts_spaces_around_eq(self):
        # ts(name = mtzsl_boatlock_op) — spaces around '='
        ast = parse_expr("ts(name = mtzsl_boatlock_op)")
        assert isinstance(ast, FuncCall) and ast.kwargs["name"] == "mtzsl_boatlock_op"

    # ---- comparisons ---

    def test_ts_comparison(self):
        ast = parse_trigger("ts(name=mscs_op) >= 1.0")
        assert isinstance(ast, CmpOp) and ast.op == ">="
        assert isinstance(ast.left, FuncCall) and ast.left.kwargs["name"] == "mscs_op"
        assert ast.right == Num(1.0)

    def test_chan_stage_comparison(self):
        # (chan_stage(channel=512,dist=6038) - chan_stage(channel=513,dist=0)) > 0.3
        ast = parse_trigger(
            "(chan_stage(channel=512,dist=6038) - chan_stage(channel=513,dist=0)) > 0.3"
        )
        assert isinstance(ast, CmpOp) and ast.op == ">"
        lhs = ast.left
        assert isinstance(lhs, BinOp) and lhs.op == "-"
        assert isinstance(lhs.left, FuncCall) and lhs.left.name == "chan_stage"
        # channel kwarg is stored as float from the NUMBER token
        assert int(lhs.left.kwargs["channel"]) == 512

    # ---- logical operators ---

    def test_and_or_precedence(self):
        # A OR B AND C  →  A OR (B AND C)  (AND binds tighter)
        ast = parse_trigger("mscs_a OR mscs_b AND mscs_c")
        assert isinstance(ast, LogOp) and ast.op == "OR"
        assert isinstance(ast.right, LogOp) and ast.right.op == "AND"

    def test_not_and(self):
        ast = parse_trigger("NOT A AND B")
        assert isinstance(ast, LogOp) and ast.op == "AND"
        assert isinstance(ast.left, UnaryOp) and ast.left.op == "NOT"

    # ---- complex real-world expression ---

    def test_ts_sub_cmp_and(self):
        # (ts(name=mscs_op)-1.0) < 0.0001 AND (ts(name=mscs_op)-1.0) > -0.0001
        ast = parse_trigger(
            "(ts(name=mscs_op)-1.0) < 0.0001 AND (ts(name=mscs_op)-1.0) > -0.0001"
        )
        assert isinstance(ast, LogOp) and ast.op == "AND"

    def test_min2_complex(self):
        # MIN2( ts(name=clfct_height)/(chan_stage(channel=232,dist=0)+13.2), 1)*0.8 + 0.75
        ast = parse_expr(
            "MIN2( ts(name=clfct_height)/(chan_stage(channel=232,dist=0)+13.2), 1)*0.8 + 0.75"
        )
        assert isinstance(ast, BinOp) and ast.op == "+"
        mul_node = ast.left
        assert isinstance(mul_node, BinOp) and mul_node.op == "*"
        assert isinstance(mul_node.left, FuncCall) and mul_node.left.name == "MIN2"

    # ---- action parser ---

    def test_simple_action(self):
        specs = parse_action("SET gate_op(gate=FalseBarrier,device=radial_gates) TO 1.0")
        assert len(specs) == 1
        s = specs[0]
        assert s.variable == "gate_op"
        assert s.kwargs["gate"] == "FalseBarrier"
        assert s.kwargs["device"] == "radial_gates"
        assert s.value_ast == Num(1.0)
        assert s.ramp_min is None

    def test_action_ramp(self):
        specs = parse_action("SET gate_op(gate=X,device=Y) TO 0.0 RAMP 120 MIN")
        assert len(specs) == 1
        assert specs[0].ramp_min == 120.0

    def test_action_while(self):
        specs = parse_action(
            "SET gate_op(gate=A,device=B) TO 1.0 WHILE SET gate_op(gate=A,device=C) TO 0.0"
        )
        assert len(specs) == 2
        assert specs[0].kwargs["device"] == "B"
        assert specs[1].kwargs["device"] == "C"

    def test_action_then(self):
        specs = parse_action(
            "SET gate_install(gate=G) TO 1.0 THEN SET gate_install(gate=G) TO 0.0"
        )
        assert len(specs) == 2

    # ---- unsupported constructs ---

    def test_season_raises(self):
        with pytest.raises(UnsupportedFeatureError):
            parse_trigger("SEASON >= 28MAY")

    def test_accumulate_raises(self):
        with pytest.raises(UnsupportedFeatureError):
            parse_expr("ACCUMULATE(ts(name=X), 0.0)")

    def test_refseason_in_trigger_raises(self):
        with pytest.raises(UnsupportedFeatureError):
            parse_trigger("DATETIME >= 28MAY")


# ---------------------------------------------------------------------------
# Evaluator tests
# ---------------------------------------------------------------------------


class TestEvaluator:

    def _eval(self, ast, time_idx=None, ts_data=None, expressions=None):
        if time_idx is None:
            time_idx = _time_idx()
        return eval_node(
            ast,
            time_idx=time_idx,
            ts_data=ts_data or {},
            expressions=expressions or {},
            hydro=None,
            hydro_cache={},
            ts_cache={},
        )

    def test_num_scalar(self):
        assert self._eval(Num(3.5)) == 3.5

    def test_bool_lit(self):
        assert self._eval(BoolLit(True)) is True
        assert self._eval(BoolLit(False)) is False

    def test_date_lit_scalar(self):
        ts = pd.Timestamp("2018-09-20 12:00")
        result = self._eval(DateTimeLit(ts))
        assert result == ts

    def test_datetime_now_returns_series(self):
        tidx = _time_idx("2015-01-01", periods=10)
        result = self._eval(DateTimeNow(), time_idx=tidx)
        assert isinstance(result, pd.Series)
        assert len(result) == 10
        assert result.iloc[0] == tidx[0]

    def test_arithmetic_add(self):
        ast = BinOp("+", Num(2.0), Num(3.0))
        assert self._eval(ast) == 5.0

    def test_arithmetic_power(self):
        ast = BinOp("^", Num(2.0), Num(10.0))
        assert self._eval(ast) == 1024.0

    def test_unary_neg(self):
        assert self._eval(UnaryOp("NEG", Num(5.0))) == -5.0

    def test_comparison_numeric(self):
        tidx = _time_idx(periods=4)
        ast = CmpOp(">", Num(3.0), Num(2.0))
        result = self._eval(ast, time_idx=tidx)
        # scalar > scalar = bool scalar (True)
        assert result is True or result == True  # noqa: E712

    def test_comparison_datetime_threshold(self):
        # DATETIME >= 20SEP2018  — check before/after boundary
        tidx = pd.date_range("2018-09-19", periods=5, freq="12h")
        ast = CmpOp(">=", DateTimeNow(), DateTimeLit(pd.Timestamp("2018-09-20")))
        result = self._eval(ast, time_idx=tidx)
        assert isinstance(result, pd.Series)
        # Before: 2018-09-19 00:00 and 2018-09-19 12:00 → False
        assert not result.iloc[0]
        assert not result.iloc[1]
        # At/after: 2018-09-20 00:00, ... → True
        assert result.iloc[2]

    def test_logical_and(self):
        tidx = _time_idx(periods=4)
        ast = LogOp("AND", BoolLit(True), BoolLit(False))
        result = self._eval(ast, time_idx=tidx)
        assert result is False or not result

    def test_logical_or_series(self):
        tidx = _time_idx(periods=4)
        # Build two Series with alternating True/False
        a = pd.Series([True, False, True, False], index=tidx)
        b = pd.Series([False, True, False, True], index=tidx)

        class _FakeTsRef:
            def __init__(self, s, name):
                self._s = s
                self.name = name
            def get_data(self): return self._s

        ts_data = {"a": _FakeTsRef(a.astype(float), "a"), "b": _FakeTsRef(b.astype(float), "b")}
        ast = LogOp(
            "OR",
            CmpOp(">", FuncCall("ts", [], {"name": "a"}), Num(0.5)),
            CmpOp(">", FuncCall("ts", [], {"name": "b"}), Num(0.5)),
        )
        result = self._eval(ast, time_idx=tidx, ts_data=ts_data)
        assert isinstance(result, pd.Series)
        assert result.all()

    def test_ts_lookup(self):
        tidx = _time_idx("2015-01-01", periods=3, freq="1h")
        raw = pd.Series([1.0, 2.0, 3.0], index=tidx)

        class _Ref:
            name = "myts"
            def get_data(self): return raw

        ast = FuncCall("ts", [], {"name": "myts"})
        result = self._eval(ast, time_idx=tidx, ts_data={"myts": _Ref()})
        assert isinstance(result, pd.Series)
        assert list(result.values) == [1.0, 2.0, 3.0]

    def test_min2(self):
        ast = FuncCall("MIN2", [Num(3.0), Num(7.0)], {})
        assert self._eval(ast) == 3.0

    def test_max2(self):
        ast = FuncCall("MAX2", [Num(3.0), Num(7.0)], {})
        assert self._eval(ast) == 7.0

    def test_ifelse(self):
        ast = FuncCall("IFELSE", [BoolLit(True), Num(10.0), Num(99.0)], {})
        assert self._eval(ast) == 10.0

    def test_bare_ref_resolved(self):
        # BareRef resolved from expressions dict
        expressions = {"my_expr": "1.0 + 2.0"}
        ast = BareRef("my_expr")
        result = self._eval(ast, expressions=expressions)
        assert result == 3.0

    def test_bare_ref_not_found_raises(self):
        with pytest.raises(ValueError, match="not found"):
            self._eval(BareRef("nonexistent"))

    def test_accumulate_raises(self):
        ast = FuncCall("ACCUMULATE", [Num(1.0)], {})
        with pytest.raises(UnsupportedFeatureError):
            self._eval(ast)

    def test_year_month_day_hour(self):
        tidx = pd.date_range("2015-06-15 08:00", periods=1, freq="1h")
        assert self._eval(FuncCall("YEAR", [], {}), time_idx=tidx).iloc[0] == 2015
        assert self._eval(FuncCall("MONTH", [], {}), time_idx=tidx).iloc[0] == 6
        assert self._eval(FuncCall("DAY", [], {}), time_idx=tidx).iloc[0] == 15
        assert self._eval(FuncCall("HOUR", [], {}), time_idx=tidx).iloc[0] == 8


# ---------------------------------------------------------------------------
# needs_hydro() tests
# ---------------------------------------------------------------------------


class TestNeedsHydro:

    def test_no_hydro(self):
        assert not needs_hydro("ts(name=foo) >= 1.0", {})

    def test_chan_stage_direct(self):
        assert needs_hydro("chan_stage(channel=1,dist=0) > 0.3", {})

    def test_chan_vel_direct(self):
        assert needs_hydro("chan_vel(channel=5,dist=100) < 2.0", {})

    def test_bare_ref_expands_to_hydro(self):
        expressions = {"my_expr": "chan_stage(channel=1,dist=0) > 0.3"}
        assert needs_hydro("my_expr", expressions)

    def test_bare_ref_no_hydro(self):
        expressions = {"simple": "ts(name=X) > 1.0"}
        assert not needs_hydro("simple", expressions)


# ---------------------------------------------------------------------------
# Round-trip: parse + eval on real integration-study expressions
# ---------------------------------------------------------------------------


class TestRealExpressions:
    """Smoke-tests: real expressions from the historical echo file must parse
    without raising.  We do not validate the exact AST structure here."""

    TRIGGERS = [
        "TRUE",
        "ts(name=fb_install) >= 1.0",
        "(ts(name=mscs_op)-1.0) < 0.0001 AND (ts(name=mscs_op)-1.0) > -0.0001",
        "DATETIME >= 20SEP2018 12:00",
        "DATETIME >= 01MAY2015 0000",
        "(chan_stage(channel=512,dist=6038) - chan_stage(channel=513,dist=0)) > 0.3",
        "ts(name = mtzsl_boatlock_op) >= 1.0",
    ]

    VALUE_EXPRS = [
        "1.0",
        "0.0",
        "-1*ts(name=dicu_div_151_flow)",
        "MIN2( ts(name=clfct_height)/(ts(name=myref)+13.2), 1)*0.8 + 0.75",
        "ts(name=clfct_nduplicate)",
    ]

    ACTIONS = [
        "SET gate_op(gate=FalseBarrier,device=radial_gates) TO 1.0",
        "SET gate_op(gate=FalseBarrier,device=radial_gates) TO 0.0 WHILE "
        "SET gate_op(gate=FalseBarrier,device=flashboards) TO 0.0",
        "SET gate_install(gate=FalseBarrier) TO 1.0",
        "SET gate_elev(gate=clifton_court,device=reservoir_gates) TO ts(name=clfct_height)",
        "SET gate_nduplicate(gate=clifton_court,device=reservoir_gates) TO ts(name=clfct_nduplicate)",
        "SET gate_coef(gate=decker_is_north_weir,device=weir,direction=from_node) TO 0.4",
        "SET gate_height(gate=clifton_court,device=reservoir_gates) TO ts(name=clfct_height)",
        "SET gate_op(gate=X,device=Y) TO 1.0 RAMP 120 MIN",
    ]

    @pytest.mark.parametrize("text", TRIGGERS)
    def test_trigger_parses(self, text):
        ast = parse_trigger(text)
        assert ast is not None

    @pytest.mark.parametrize("text", VALUE_EXPRS)
    def test_value_expr_parses(self, text):
        ast = parse_expr(text)
        assert ast is not None

    @pytest.mark.parametrize("text", ACTIONS)
    def test_action_parses(self, text):
        specs = parse_action(text)
        assert len(specs) >= 1
        for s in specs:
            assert isinstance(s, ActionSpec)
            assert s.variable  # non-empty
