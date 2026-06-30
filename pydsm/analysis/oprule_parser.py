"""
Lexer, Pratt parser, and evaluator for the DSM2 operating-rule expression language.

Public API
----------
parse_trigger(text) -> AST node
    Parse a boolean trigger expression (or "TRUE").

parse_expr(text) -> AST node
    Parse a numeric value expression (used in action TO clauses).

parse_action(text) -> list[ActionSpec]
    Parse a full action string: ``SET interface(kwargs) TO expr [RAMP N MIN]``
    with optional ``WHILE`` / ``THEN`` chaining. Both combinators are treated
    as simultaneous — all sub-actions are returned as a flat list.

eval_node(node, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
    Recursively evaluate an AST node.  Returns ``pd.Series``, ``float``, or
    ``bool`` depending on the node type.

UnsupportedFeatureError
    Raised (and logged as WARNING) when the parser or evaluator encounters
    an unsupported construct: SEASON, ACCUMULATE, PREDICT, PID, IPID.
"""
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exception types
# ---------------------------------------------------------------------------


class UnsupportedFeatureError(Exception):
    """Raised for DSM2 operating-rule constructs that are not yet implemented."""


class ParseError(Exception):
    """Raised when the input string cannot be parsed."""


# ---------------------------------------------------------------------------
# Token definitions
# ---------------------------------------------------------------------------

# Token types
T_NUMBER = "NUMBER"
T_NAME = "NAME"
T_KEYWORD = "KEYWORD"
T_REFDATE = "REFDATE"
T_REFTIME = "REFTIME"
T_OP = "OP"
T_EOF = "EOF"

# Keywords (case-insensitive match; stored as uppercase)
_KEYWORDS = frozenset(
    [
        "AND", "OR", "NOT", "TRUE", "FALSE",
        "SET", "TO", "WHEN", "WHILE", "THEN", "RAMP", "STEP",
        "ABS", "SQRT", "EXP", "LOG", "LN",
        "MAX2", "MIN2", "MAX3", "MIN3",
        "LOOKUP", "IFELSE",
        "DATETIME", "YEAR", "MONTH", "DAY", "HOUR", "MIN", "DT",
        "SEASON",
        "ACCUMULATE", "PREDICT", "PID", "IPID",
        "LINEAR", "QUAD",
    ]
)

# Month names → numeric value (case-insensitive)
_MONTHS = {
    "JAN": 1.0, "FEB": 2.0, "MAR": 3.0, "APR": 4.0,
    "MAY": 5.0, "JUN": 6.0, "JUL": 7.0, "AUG": 8.0,
    "SEP": 9.0, "OCT": 10.0, "NOV": 11.0, "DEC": 12.0,
}

# Month pattern for REFDATE / REFSEASON detection
_MON_PAT = "(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"

# Pre-compiled master tokeniser regex.
# Rules applied in order (first match wins).
_TOKEN_RE = re.compile(
    r"""
    (?P<SKIP>   [ \t\n\r]+)
  | (?P<REFDATE>  [0-3]?[0-9]""" + _MON_PAT + r"""[12][0-9]{3})  # e.g. 20SEP2018
  | (?P<REFSEASON>[0-3]?[0-9]""" + _MON_PAT + r""")               # e.g. 28MAY (no year)
  | (?P<REFTIME>  [0-2][0-9]:[0-5][0-9])                           # HH:MM with colon
  | (?P<NAME>     [A-Za-z][A-Za-z0-9_@]*)
  | (?P<NUMBER>   (?:[0-9]+)(?:\.[0-9]*)?(?:[eE][+-]?[0-9]+)?
                | \.[0-9]+(?:[eE][+-]?[0-9]+)?)
  | (?P<GE>       >=)
  | (?P<LE>       <=)
  | (?P<NE>       <>)
  | (?P<EQ>       ==)
  | (?P<OP>       [-+*/^()[\],<>=])
  """,
    re.VERBOSE | re.IGNORECASE,
)


@dataclass
class Token:
    type: str    # T_NUMBER, T_NAME, T_KEYWORD, T_REFDATE, T_REFTIME, T_OP, T_EOF
    value: Any   # float for NUMBER, str for everything else
    pos: int


def tokenize(text: str) -> List[Token]:
    """Lex *text* into a list of Tokens (terminated by a T_EOF)."""
    tokens: List[Token] = []
    pos = 0
    length = len(text)
    while pos < length:
        m = _TOKEN_RE.match(text, pos)
        if m is None:
            raise ParseError(f"Unexpected character {text[pos]!r} at position {pos}")
        pos = m.end()
        kind = m.lastgroup

        if kind == "SKIP":
            continue

        raw = m.group()

        if kind == "REFDATE":
            tokens.append(Token(T_REFDATE, raw.upper(), m.start()))

        elif kind == "REFSEASON":
            logger.warning(
                "REFSEASON token %r is not supported and will cause the rule to be skipped.", raw
            )
            # Emit a sentinel that the parser converts to UnsupportedFeatureError
            tokens.append(Token(T_KEYWORD, "_REFSEASON_", m.start()))

        elif kind == "REFTIME":
            tokens.append(Token(T_REFTIME, raw, m.start()))

        elif kind == "NAME":
            upper = raw.upper()
            if upper in _MONTHS:
                tokens.append(Token(T_NUMBER, float(_MONTHS[upper]), m.start()))
            elif upper in _KEYWORDS:
                tokens.append(Token(T_KEYWORD, upper, m.start()))
            else:
                tokens.append(Token(T_NAME, raw, m.start()))

        elif kind == "NUMBER":
            tokens.append(Token(T_NUMBER, float(raw), m.start()))

        elif kind in ("GE", "LE", "NE", "EQ"):
            # Multi-char comparison operators
            tokens.append(Token(T_OP, raw, m.start()))

        else:  # kind == "OP" (single-char)
            # "=" becomes the DEFINE pseudo-op; everything else keeps its char
            tokens.append(Token(T_OP, raw, m.start()))

    tokens.append(Token(T_EOF, "", length))
    return tokens


# ---------------------------------------------------------------------------
# AST node types
# ---------------------------------------------------------------------------


@dataclass
class Num:
    value: float


@dataclass
class BoolLit:
    value: bool


@dataclass
class DateTimeLit:
    """A specific point in time (from REFDATE [REFTIME])."""
    value: pd.Timestamp


@dataclass
class DateTimeNow:
    """The DATETIME keyword — evaluates to the current simulation time."""


@dataclass
class BinOp:
    op: str  # +, -, *, /, ^
    left: Any
    right: Any


@dataclass
class CmpOp:
    op: str  # >=, <=, >, <, ==, <>
    left: Any
    right: Any


@dataclass
class LogOp:
    op: str  # AND, OR
    left: Any
    right: Any


@dataclass
class UnaryOp:
    op: str  # NEG, NOT
    operand: Any


@dataclass
class FuncCall:
    """A named function / model variable call: ``ts(name=X)``, ``MIN2(a,b)``, ..."""
    name: str
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BareRef:
    """A bare identifier resolved as an OPRULE_EXPRESSION name at eval time."""
    name: str


@dataclass
class ActionSpec:
    variable: str           # e.g. "gate_op", "gate_coef", "gate_height"
    kwargs: Dict[str, str]  # e.g. {"gate": "FalseBarrier", "device": "radial_gates"}
    value_ast: Any          # parsed AST of the value expression
    ramp_min: Optional[float]  # from RAMP N MIN (stored; ramp interpolation not applied)


# ---------------------------------------------------------------------------
# Parser (Pratt / top-down operator precedence)
# ---------------------------------------------------------------------------

# Left binding powers for infix operators
_LEFT_BP: Dict[str, int] = {
    "OR": 10,
    "AND": 20,
    "<": 30, "<=": 30, ">": 30, ">=": 30, "==": 30, "<>": 30,
    "+": 40, "-": 40,
    "*": 50, "/": 50,
    "^": 60,  # right-assoc — see _right_bp
}
# Right binding power for right-associative operators
_RIGHT_BP: Dict[str, int] = {"^": 59}

# Function keywords that take argument lists
_FUNC_KEYWORDS = frozenset(
    ["ABS", "SQRT", "EXP", "LOG", "LN",
     "MAX2", "MIN2", "MAX3", "MIN3", "LOOKUP", "IFELSE",
     "YEAR", "MONTH", "DAY", "HOUR", "MIN", "DT",
     "ACCUMULATE", "PREDICT", "PID", "IPID"]
)

# Month abbreviations for parsing REFDATE strings
_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_REFDATE_RE = re.compile(
    r"^([0-3]?[0-9])(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)([12][0-9]{3})$",
    re.IGNORECASE,
)


def _parse_refdate(raw: str, time_str: Optional[str] = None) -> pd.Timestamp:
    """Convert a REFDATE string (e.g. '20SEP2018') + optional REFTIME to Timestamp."""
    m = _REFDATE_RE.match(raw)
    if not m:
        raise ParseError(f"Cannot parse REFDATE: {raw!r}")
    day = int(m.group(1))
    mon = _MONTH_MAP[m.group(2).upper()]
    year = int(m.group(3))
    hour, minute = 0, 0
    if time_str:
        if ":" in time_str:
            h_str, mi_str = time_str.split(":")
            hour, minute = int(h_str), int(mi_str)
        else:
            # 4-digit HHMM
            hhmm = int(time_str)
            hour, minute = hhmm // 100, hhmm % 100
    return pd.Timestamp(year=year, month=mon, day=day, hour=hour, minute=minute)


class _Parser:
    """Pratt (top-down operator precedence) parser for DSM2 operating-rule expressions."""

    def __init__(self, tokens: List[Token]):
        self._tokens = tokens
        self._pos = 0

    # --- Token access ---

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        t = self._tokens[self._pos]
        self._pos += 1
        return t

    def _expect_op(self, value: str) -> Token:
        t = self._peek()
        if t.type != T_OP or t.value != value:
            raise ParseError(
                f"Expected operator {value!r} at pos {t.pos}, got {t.type}({t.value!r})"
            )
        return self._advance()

    def _expect_keyword(self, kw: str) -> Token:
        t = self._peek()
        if t.type != T_KEYWORD or t.value != kw:
            raise ParseError(
                f"Expected keyword {kw!r} at pos {t.pos}, got {t.type}({t.value!r})"
            )
        return self._advance()

    # --- Left binding power ---

    def _lbp(self, tok: Token) -> int:
        if tok.type == T_KEYWORD and tok.value in _LEFT_BP:
            return _LEFT_BP[tok.value]
        if tok.type == T_OP and tok.value in _LEFT_BP:
            return _LEFT_BP[tok.value]
        return 0

    # --- Pratt core ---

    def parse_bp(self, min_bp: int) -> Any:
        left = self._parse_atom()
        while True:
            tok = self._peek()
            bp = self._lbp(tok)
            if bp <= min_bp:
                break
            self._advance()
            right_bp = _RIGHT_BP.get(tok.value, bp)
            right = self.parse_bp(right_bp)
            op_val = tok.value.upper() if tok.type == T_KEYWORD else tok.value
            if op_val in ("AND", "OR"):
                left = LogOp(op_val, left, right)
            elif op_val in ("<", "<=", ">", ">=", "==", "<>"):
                left = CmpOp(op_val, left, right)
            else:
                left = BinOp(op_val, left, right)
        return left

    def _parse_atom(self) -> Any:
        tok = self._peek()

        # --- NUMBER literal ---
        if tok.type == T_NUMBER:
            self._advance()
            return Num(tok.value)

        # --- TRUE / FALSE ---
        if tok.type == T_KEYWORD and tok.value == "TRUE":
            self._advance()
            return BoolLit(True)
        if tok.type == T_KEYWORD and tok.value == "FALSE":
            self._advance()
            return BoolLit(False)

        # --- DATETIME keyword (current simulation time) ---
        if tok.type == T_KEYWORD and tok.value == "DATETIME":
            self._advance()
            return DateTimeNow()

        # --- REFDATE [REFTIME | 4-digit NUMBER] ---
        if tok.type == T_REFDATE:
            self._advance()
            time_str = None
            nxt = self._peek()
            if nxt.type == T_REFTIME:
                time_str = nxt.value
                self._advance()
            elif nxt.type == T_NUMBER and _is_valid_hhmm(nxt.value):
                time_str = f"{int(nxt.value):04d}"
                self._advance()
            return DateTimeLit(_parse_refdate(tok.value, time_str))

        # --- Unsupported: REFSEASON sentinel ---
        if tok.type == T_KEYWORD and tok.value == "_REFSEASON_":
            raise UnsupportedFeatureError(
                "REFSEASON tokens are not supported. Skipping rule."
            )

        # --- SEASON keyword ---
        if tok.type == T_KEYWORD and tok.value == "SEASON":
            self._advance()
            raise UnsupportedFeatureError("SEASON is not supported. Skipping rule.")

        # --- Unsupported stateful ops as function calls ---
        if tok.type == T_KEYWORD and tok.value in ("ACCUMULATE", "PREDICT", "PID", "IPID"):
            self._advance()
            raise UnsupportedFeatureError(
                f"{tok.value}() is not supported (stateful operator). Skipping rule."
            )

        # --- Function keywords with arglist ---
        if tok.type == T_KEYWORD and tok.value in _FUNC_KEYWORDS:
            self._advance()
            name = tok.value
            self._expect_op("(")
            args, kwargs = self._parse_arglist()
            self._expect_op(")")
            return FuncCall(name, args, kwargs)

        # --- NAME: either a function call or a bare reference ---
        if tok.type == T_NAME:
            self._advance()
            name = tok.value
            if self._peek().type == T_OP and self._peek().value == "(":
                self._advance()  # consume '('
                args, kwargs = self._parse_arglist()
                self._expect_op(")")
                return FuncCall(name, args, kwargs)
            return BareRef(name)

        # --- Grouped sub-expression ---
        if tok.type == T_OP and tok.value == "(":
            self._advance()
            inner = self.parse_bp(0)
            self._expect_op(")")
            return inner

        # --- Unary minus ---
        if tok.type == T_OP and tok.value == "-":
            self._advance()
            operand = self.parse_bp(70)  # unary has highest precedence
            return UnaryOp("NEG", operand)

        # --- Unary NOT ---
        if tok.type == T_KEYWORD and tok.value == "NOT":
            self._advance()
            operand = self.parse_bp(70)
            return UnaryOp("NOT", operand)

        raise ParseError(
            f"Unexpected token {tok.type}({tok.value!r}) at pos {tok.pos}"
        )

    # --- Arglist parser: NAME=value kwargs or positional args ---

    def _parse_arglist(self) -> tuple:
        """Parse an arglist until ')' or ']'.  Returns (positional_args, keyword_args)."""
        args: List[Any] = []
        kwargs: Dict[str, Any] = {}
        tok = self._peek()
        if tok.type == T_OP and tok.value in (")", "]"):
            return args, kwargs

        while True:
            # Check for NAME = value  (keyword argument)
            nxt = self._peek()
            if nxt.type == T_NAME:
                # Peek ahead to see if followed by '='
                if self._pos + 1 < len(self._tokens):
                    after = self._tokens[self._pos + 1]
                    if after.type == T_OP and after.value == "=":
                        kw_name = nxt.value
                        self._advance()   # consume NAME
                        self._advance()   # consume '='
                        val_tok = self._peek()
                        if val_tok.type == T_NAME:
                            kwargs[kw_name] = val_tok.value
                            self._advance()
                        elif val_tok.type == T_NUMBER:
                            kwargs[kw_name] = val_tok.value  # float
                            self._advance()
                        else:
                            raise ParseError(
                                f"Expected NAME or NUMBER after '=' in arglist, "
                                f"got {val_tok.type}({val_tok.value!r}) at pos {val_tok.pos}"
                            )
                        # check for separator
                        sep = self._peek()
                        if sep.type == T_OP and sep.value == ",":
                            self._advance()
                            continue
                        break

            # Array literal  [ NUMBER, NUMBER, ... ]
            if nxt.type == T_OP and nxt.value == "[":
                self._advance()  # consume '['
                arr = []
                while True:
                    t = self._peek()
                    if t.type == T_NUMBER:
                        arr.append(t.value)
                        self._advance()
                    elif t.type == T_OP and t.value == ",":
                        self._advance()
                    elif t.type == T_OP and t.value == "]":
                        self._advance()
                        break
                    else:
                        raise ParseError(f"Unexpected token in array literal: {t!r}")
                args.append(arr)
                sep = self._peek()
                if sep.type == T_OP and sep.value == ",":
                    self._advance()
                    continue
                break

            # Positional expression argument
            arg_ast = self.parse_bp(0)
            args.append(arg_ast)
            sep = self._peek()
            if sep.type == T_OP and sep.value == ",":
                self._advance()
                continue
            break

        return args, kwargs

    # --- Action parser ---

    def parse_action(self) -> List[ActionSpec]:
        """Parse ``SET interface(kwargs) TO expr [RAMP N MIN] [WHILE|THEN ...]``."""
        specs: List[ActionSpec] = []
        self._parse_single_action(specs)
        return specs

    def _parse_single_action(self, specs: List[ActionSpec]) -> None:
        self._expect_keyword("SET")

        # interface name — NAME or a keyword used as interface name
        tok = self._peek()
        if tok.type == T_NAME:
            name = tok.value
            self._advance()
        elif tok.type == T_KEYWORD:
            # Some interface names (like 'gate_op') may look like keywords
            name = tok.value.lower()
            self._advance()
        else:
            raise ParseError(f"Expected interface name after SET at pos {tok.pos}")

        # Optional arglist
        kwargs: Dict[str, Any] = {}
        if self._peek().type == T_OP and self._peek().value == "(":
            self._advance()  # consume '('
            _, kwargs = self._parse_arglist()
            self._expect_op(")")

        self._expect_keyword("TO")
        value_ast = self.parse_bp(0)

        # Optional RAMP N MIN
        ramp_min: Optional[float] = None
        if self._peek().type == T_KEYWORD and self._peek().value == "RAMP":
            self._advance()  # consume RAMP
            ramp_tok = self._peek()
            if ramp_tok.type != T_NUMBER:
                raise ParseError(f"Expected number after RAMP at pos {ramp_tok.pos}")
            ramp_min = ramp_tok.value
            self._advance()
            # Expect MIN (or HOUR)
            unit_tok = self._peek()
            if unit_tok.type == T_KEYWORD and unit_tok.value in ("MIN", "HOUR"):
                self._advance()  # consume unit
                if unit_tok.value == "HOUR":
                    ramp_min = ramp_min * 60.0

        # Normalise kwargs: float values for known numeric keys (channel, dist)
        norm_kwargs: Dict[str, str] = {}
        for k, v in kwargs.items():
            if isinstance(v, float):
                norm_kwargs[k.lower()] = str(int(v)) if v == int(v) else str(v)
            else:
                norm_kwargs[k.lower()] = str(v)

        specs.append(ActionSpec(name.lower(), norm_kwargs, value_ast, ramp_min))

        # WHILE / THEN chains — treat both as simultaneous
        nxt = self._peek()
        if nxt.type == T_KEYWORD and nxt.value in ("WHILE", "THEN"):
            self._advance()
            self._parse_single_action(specs)


def _is_valid_hhmm(val: float) -> bool:
    """Return True if *val* (a NUMBER token value) looks like a valid HHMM time."""
    if val < 0 or val > 2359:
        return False
    hh = int(val) // 100
    mm = int(val) % 100
    return hh < 24 and mm < 60


# ---------------------------------------------------------------------------
# Public parse entry points
# ---------------------------------------------------------------------------


def parse_trigger(text: str) -> Any:
    """Parse a DSM2 OPERATING_RULE TRIGGER string into an AST node.

    Accepts ``"TRUE"`` / ``"FALSE"`` literals, boolean expressions, and
    arithmetic comparisons.

    Raises
    ------
    ParseError
        If the text cannot be parsed.
    UnsupportedFeatureError
        If the text contains unsupported constructs (SEASON, ACCUMULATE, etc.).
    """
    tokens = tokenize(text.strip())
    p = _Parser(tokens)
    ast = p.parse_bp(0)
    if p._peek().type != T_EOF:
        raise ParseError(
            f"Unexpected tokens after expression: {p._peek()!r} in {text!r}"
        )
    return ast


def parse_expr(text: str) -> Any:
    """Parse a numeric value expression (used in TO clauses) into an AST node."""
    tokens = tokenize(text.strip())
    p = _Parser(tokens)
    ast = p.parse_bp(0)
    if p._peek().type != T_EOF:
        raise ParseError(
            f"Unexpected tokens after expression: {p._peek()!r} in {text!r}"
        )
    return ast


def parse_action(text: str) -> List[ActionSpec]:
    """Parse a DSM2 OPERATING_RULE ACTION string into a list of :class:`ActionSpec`.

    Handles ``WHILE``/``THEN``-chained compound actions (both treated as
    simultaneous).  Any ``RAMP N MIN`` suffix is parsed and stored in
    :attr:`ActionSpec.ramp_min` but ramp-interpolation is not applied.
    """
    tokens = tokenize(text.strip())
    p = _Parser(tokens)
    specs = p.parse_action()
    return specs


# ---------------------------------------------------------------------------
# Resample helpers (shared with gate_state.py via import from here)
# ---------------------------------------------------------------------------


def _resample_series_to_index(series: pd.Series, time_idx: pd.DatetimeIndex) -> pd.Series:
    """Forward-fill (then back-fill for leading NaN) an irregular Series onto *time_idx*."""
    if not isinstance(series.index, pd.DatetimeIndex):
        series.index = pd.to_datetime(series.index)
    series = series[~series.index.duplicated(keep="last")]
    combined = series.reindex(series.index.union(time_idx)).ffill().bfill()
    return combined.reindex(time_idx)


def _resample_to_index(ts_ref, time_idx: pd.DatetimeIndex, _ts_cache: dict = None) -> pd.Series:
    """Load a DSM2TimeSeriesReference and resample to *time_idx*.

    *_ts_cache* is an optional dict keyed by ``ts_ref.name`` that avoids
    re-reading the same DSS path when the same reference appears in multiple
    OPERATING_RULE triggers.
    """
    if _ts_cache is not None and ts_ref.name in _ts_cache:
        data = _ts_cache[ts_ref.name]
    else:
        data = ts_ref.get_data()
        if _ts_cache is not None:
            _ts_cache[ts_ref.name] = data
    if not isinstance(data, pd.Series):
        return pd.Series(float(data), index=time_idx)
    return _resample_series_to_index(data, time_idx)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

# Supported comparison operators
_CMP_DISPATCH = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    "==": lambda a, b: a == b,
    "<>": lambda a, b: a != b,
}

# Supported arithmetic operators
_ARITH_DISPATCH = {
    "+": lambda a, b: a + b,
    "-": lambda a, b: a - b,
    "*": lambda a, b: a * b,
    "/": lambda a, b: a / b,
    "^": lambda a, b: a ** b,
}


def _broadcast(a, b, op_fn):
    """Apply *op_fn(a, b)* broadcasting scalar/Series combinations."""
    if isinstance(a, pd.Series) or isinstance(b, pd.Series):
        # Ensure both are Series or broadcastable
        return op_fn(a, b)
    return op_fn(a, b)


def eval_node(
    node: Any,
    time_idx: pd.DatetimeIndex,
    ts_data: dict,
    expressions: dict,
    hydro,
    hydro_cache: dict,
    ts_cache: dict,
):
    """Recursively evaluate an AST node.

    Parameters
    ----------
    node : AST node
        Output of :func:`parse_trigger`, :func:`parse_expr`, or any sub-node.
    time_idx : pd.DatetimeIndex
        The evaluation time axis for this chunk.
    ts_data : dict[str, DSM2TimeSeriesReference]
        OPRULE_TIME_SERIES name → reference objects.
    expressions : dict[str, str]
        OPRULE_EXPRESSION name → definition string.
    hydro : HydroH5 | None
        Required when the trigger involves ``chan_stage`` / ``chan_vel``.
    hydro_cache : dict
        Memoization cache for HDF5 channel reads (keyed by (kind, channel, loc)).
    ts_cache : dict
        Memoization cache for DSS time series (keyed by ts name).

    Returns
    -------
    pd.Series, float, bool, or pd.Timestamp
    """
    # --- Scalar literals ---
    if isinstance(node, Num):
        return node.value

    if isinstance(node, BoolLit):
        return node.value

    if isinstance(node, DateTimeLit):
        return node.value  # pd.Timestamp scalar

    if isinstance(node, DateTimeNow):
        # Return a Series of pd.Timestamps (one per row in time_idx)
        return pd.Series(time_idx, index=time_idx)

    # --- Binary arithmetic ---
    if isinstance(node, BinOp):
        a = eval_node(node.left, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
        b = eval_node(node.right, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
        op_fn = _ARITH_DISPATCH.get(node.op)
        if op_fn is None:
            raise ValueError(f"Unknown binary operator: {node.op!r}")
        return _broadcast(a, b, op_fn)

    # --- Comparison ---
    if isinstance(node, CmpOp):
        a = eval_node(node.left, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
        b = eval_node(node.right, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
        cmp_fn = _CMP_DISPATCH.get(node.op)
        if cmp_fn is None:
            raise ValueError(f"Unknown comparison operator: {node.op!r}")
        # Timestamp comparison: if either side is a Timestamp or a Series of Timestamps
        a_is_ts = _is_timestamp_type(a)
        b_is_ts = _is_timestamp_type(b)
        if a_is_ts or b_is_ts:
            # Normalise both to a common Timestamp comparable type
            a_cmp = _to_timestamp_comparable(a, time_idx)
            b_cmp = _to_timestamp_comparable(b, time_idx)
            return pd.Series(cmp_fn(a_cmp, b_cmp), index=time_idx, dtype=bool)
        return _broadcast(a, b, cmp_fn)

    # --- Logical ---
    if isinstance(node, LogOp):
        a = eval_node(node.left, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
        b = eval_node(node.right, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
        if node.op == "AND":
            return _broadcast(a, b, lambda x, y: x & y)
        if node.op == "OR":
            return _broadcast(a, b, lambda x, y: x | y)
        raise ValueError(f"Unknown logical operator: {node.op!r}")

    # --- Unary ---
    if isinstance(node, UnaryOp):
        v = eval_node(node.operand, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)
        if node.op == "NEG":
            return -v
        if node.op == "NOT":
            if isinstance(v, pd.Series):
                return ~v.astype(bool)
            return not v
        raise ValueError(f"Unknown unary operator: {node.op!r}")

    # --- Named bare reference (OPRULE_EXPRESSION) ---
    if isinstance(node, BareRef):
        name = node.name
        if name in expressions:
            try:
                sub_ast = parse_trigger(expressions[name])
                return eval_node(
                    sub_ast, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache
                )
            except ParseError:
                sub_ast = parse_expr(expressions[name])
                return eval_node(
                    sub_ast, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache
                )
        raise ValueError(
            f"BareRef {name!r} not found in OPRULE_EXPRESSION dict. "
            "Known: " + str(list(expressions.keys())[:10])
        )

    # --- Function calls ---
    if isinstance(node, FuncCall):
        return _eval_func(node, time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache)

    raise TypeError(f"Unknown AST node type: {type(node).__name__}")


def _is_timestamp_type(v) -> bool:
    """Return True if *v* is a pd.Timestamp or a Series of Timestamps."""
    if isinstance(v, pd.Timestamp):
        return True
    if isinstance(v, pd.Series) and len(v) > 0:
        return isinstance(v.iloc[0], pd.Timestamp)
    return False


def _to_timestamp_comparable(v, time_idx: pd.DatetimeIndex):
    """Convert *v* to something pd.Timestamp comparison can handle.

    A scalar Timestamp stays as-is.  A Series of Timestamps becomes a
    DatetimeIndex for vectorised comparison.
    """
    if isinstance(v, pd.Timestamp):
        return v
    if isinstance(v, pd.Series):
        return pd.DatetimeIndex(v.values)
    return v


def _eval_func(
    node: FuncCall,
    time_idx: pd.DatetimeIndex,
    ts_data: dict,
    expressions: dict,
    hydro,
    hydro_cache: dict,
    ts_cache: dict,
):
    """Evaluate a FuncCall node."""
    name = node.name.upper()

    # --- ts(name=X) — OPRULE_TIME_SERIES lookup ---
    if name == "TS":
        ts_name = node.kwargs.get("name")
        if ts_name is None and node.args:
            ts_name = str(node.args[0])
        if ts_name is None:
            raise ValueError("ts() requires name=<identifier>")
        if ts_name not in ts_data:
            raise KeyError(f"OPRULE_TIME_SERIES name not found: {ts_name!r}")
        return _resample_to_index(ts_data[ts_name], time_idx, ts_cache)

    # --- chan_stage / chan_vel ---
    if name in ("CHAN_STAGE", "CHAN_VEL"):
        if hydro is None:
            raise ValueError(
                f"{name}() requires HDF5 data but hydro=None. Provide --hydro-file."
            )
        channel_id = node.kwargs.get("channel")
        dist = node.kwargs.get("dist", "0")
        if channel_id is None:
            raise ValueError(f"{name}() missing 'channel' argument")
        kind = "stage" if name == "CHAN_STAGE" else "vel"
        return _eval_chan_numeric_new(
            kind, int(channel_id), float(dist), time_idx, hydro, hydro_cache
        )

    # --- Unsupported stateful operators ---
    if name in ("ACCUMULATE", "PREDICT", "PID", "IPID"):
        logger.warning("%s() is not supported. Skipping rule.", name)
        raise UnsupportedFeatureError(
            f"{name}() is a stateful operator and is not supported."
        )

    # --- Helper to evaluate positional args ---
    def _eval_arg(i):
        return eval_node(
            node.args[i], time_idx, ts_data, expressions, hydro, hydro_cache, ts_cache
        )

    # --- Math unary functions ---
    if name == "ABS":
        return np.abs(_eval_arg(0))
    if name == "SQRT":
        return np.sqrt(_eval_arg(0))
    if name == "EXP":
        return np.exp(_eval_arg(0))
    if name == "LN":
        return np.log(_eval_arg(0))
    if name == "LOG":
        return np.log10(_eval_arg(0))  # DSM2 LOG = base-10

    # --- Binary min/max ---
    if name == "MIN2":
        a, b = _eval_arg(0), _eval_arg(1)
        return np.minimum(a, b)
    if name == "MAX2":
        a, b = _eval_arg(0), _eval_arg(1)
        return np.maximum(a, b)

    # --- Ternary min/max ---
    if name == "MIN3":
        a, b, c = _eval_arg(0), _eval_arg(1), _eval_arg(2)
        return np.minimum(a, np.minimum(b, c))
    if name == "MAX3":
        a, b, c = _eval_arg(0), _eval_arg(1), _eval_arg(2)
        return np.maximum(a, np.maximum(b, c))

    # --- IFELSE(cond, true_val, false_val) ---
    if name == "IFELSE":
        cond = _eval_arg(0)
        t_val = _eval_arg(1)
        f_val = _eval_arg(2)
        if isinstance(cond, pd.Series):
            return np.where(cond.values, t_val, f_val)
        return t_val if cond else f_val

    # --- LOOKUP(x, [x0,x1,...], [y0,y1,...]) ---
    if name == "LOOKUP":
        x = _eval_arg(0)
        xp = node.args[1]  # list of floats (raw array from parser)
        fp = node.args[2]
        if not isinstance(xp, list) or not isinstance(fp, list):
            raise ValueError("LOOKUP requires array literals [x0,...] and [y0,...]")
        return np.interp(x, xp, fp)

    # --- Time component accessors ---
    if name == "YEAR":
        return pd.Series(time_idx.year, index=time_idx, dtype=float)
    if name == "MONTH":
        return pd.Series(time_idx.month, index=time_idx, dtype=float)
    if name == "DAY":
        return pd.Series(time_idx.day, index=time_idx, dtype=float)
    if name == "HOUR":
        return pd.Series(time_idx.hour, index=time_idx, dtype=float)
    if name == "MIN":
        return pd.Series(time_idx.minute, index=time_idx, dtype=float)
    if name == "DT":
        # Timestep in minutes inferred from time_idx frequency
        freq = time_idx.freq
        if freq is not None:
            dt_minutes = pd.tseries.frequencies.to_offset(freq).nanos / 1e9 / 60.0
        else:
            dt_minutes = (
                (time_idx[1] - time_idx[0]).total_seconds() / 60.0
                if len(time_idx) >= 2
                else 60.0
            )
        return float(dt_minutes)

    raise ValueError(f"Unknown function: {node.name!r}")


# ---------------------------------------------------------------------------
# Channel HDF5 reader (used by _eval_func above)
# ---------------------------------------------------------------------------


def _eval_chan_numeric_new(
    kind: str,
    channel_id: int,
    dist: float,
    time_idx: pd.DatetimeIndex,
    hydro,
    hydro_cache: dict,
) -> pd.Series:
    """Read chan_stage or chan_vel from the HDF5 for a single channel / location."""
    location = "upstream" if dist == 0 else "downstream"
    cache_key = (kind, channel_id, location)
    if cache_key in hydro_cache:
        return _resample_series_to_index(hydro_cache[cache_key], time_idx)

    if kind == "stage":
        df = hydro.get_channel_stage(str(channel_id), location)
    else:
        # velocity = flow / area
        flow_df = hydro.get_channel_flow(str(channel_id), location)
        area_df = hydro.get_channel_area(str(channel_id), location)
        flow = flow_df.iloc[:, 0] if isinstance(flow_df, pd.DataFrame) else flow_df
        area = area_df.iloc[:, 0] if isinstance(area_df, pd.DataFrame) else area_df
        if isinstance(flow.index, pd.PeriodIndex):
            flow.index = flow.index.to_timestamp()
        if isinstance(area.index, pd.PeriodIndex):
            area.index = area.index.to_timestamp()
        series = flow / area.where(area != 0, other=np.nan)
        hydro_cache[cache_key] = series
        return _resample_series_to_index(series, time_idx)

    series = df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df
    if isinstance(series.index, pd.PeriodIndex):
        series.index = series.index.to_timestamp()
    hydro_cache[cache_key] = series
    return _resample_series_to_index(series, time_idx)


# ---------------------------------------------------------------------------
# Walk AST for hydro dependency detection
# ---------------------------------------------------------------------------


def _ast_needs_hydro(node: Any) -> bool:
    """Return True if *node* (or any descendant) calls chan_stage or chan_vel."""
    if isinstance(node, FuncCall):
        if node.name.upper() in ("CHAN_STAGE", "CHAN_VEL"):
            return True
        for a in node.args:
            if not isinstance(a, list) and _ast_needs_hydro(a):
                return True
        return False
    if isinstance(node, (BinOp, CmpOp, LogOp)):
        return _ast_needs_hydro(node.left) or _ast_needs_hydro(node.right)
    if isinstance(node, UnaryOp):
        return _ast_needs_hydro(node.operand)
    return False


def needs_hydro(text: str, expressions: dict, _visited: set = None) -> bool:
    """Return True if *text* (or any OPRULE_EXPRESSION it references) needs HDF5 data.

    Replaces the regex-based ``_needs_hydro`` in gate_state.py.
    """
    if _visited is None:
        _visited = set()
    try:
        ast = parse_trigger(text)
    except (ParseError, UnsupportedFeatureError):
        try:
            ast = parse_expr(text)
        except (ParseError, UnsupportedFeatureError):
            return False

    if _ast_needs_hydro(ast):
        return True

    # Expand bare-name references into expressions
    for node in _iter_nodes(ast):
        if isinstance(node, BareRef) and node.name in expressions and node.name not in _visited:
            _visited.add(node.name)
            if needs_hydro(expressions[node.name], expressions, _visited):
                return True
    return False


def _iter_nodes(node: Any):
    """Yield all AST nodes in *node* (depth-first)."""
    yield node
    if isinstance(node, (BinOp, CmpOp, LogOp)):
        yield from _iter_nodes(node.left)
        yield from _iter_nodes(node.right)
    elif isinstance(node, UnaryOp):
        yield from _iter_nodes(node.operand)
    elif isinstance(node, FuncCall):
        for a in node.args:
            if not isinstance(a, list):
                yield from _iter_nodes(a)
