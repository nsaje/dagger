// parser grammar for alarms

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/nsaje/dagger/dagger"
)

var g = &grammar{
	rules: []*rule{
		{
			name: "AlarmDefinition",
			pos:  position{line: 8, col: 1, offset: 49},
			expr: &actionExpr{
				pos: position{line: 8, col: 20, offset: 68},
				run: (*parser).callonAlarmDefinition1,
				expr: &seqExpr{
					pos: position{line: 8, col: 20, offset: 68},
					exprs: []interface{}{
						&ruleRefExpr{
							pos:  position{line: 8, col: 20, offset: 68},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 8, col: 22, offset: 70},
							label: "e",
							expr: &ruleRefExpr{
								pos:  position{line: 8, col: 24, offset: 72},
								name: "Expr",
							},
						},
						&ruleRefExpr{
							pos:  position{line: 8, col: 29, offset: 77},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 8, col: 31, offset: 79},
							label: "mb",
							expr: &zeroOrOneExpr{
								pos: position{line: 8, col: 34, offset: 82},
								expr: &ruleRefExpr{
									pos:  position{line: 8, col: 34, offset: 82},
									name: "MatchBy",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "MatchBy",
			pos:  position{line: 16, col: 1, offset: 213},
			expr: &actionExpr{
				pos: position{line: 16, col: 12, offset: 224},
				run: (*parser).callonMatchBy1,
				expr: &seqExpr{
					pos: position{line: 16, col: 12, offset: 224},
					exprs: []interface{}{
						&litMatcher{
							pos:        position{line: 16, col: 12, offset: 224},
							val:        ",",
							ignoreCase: false,
						},
						&ruleRefExpr{
							pos:  position{line: 16, col: 16, offset: 228},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 16, col: 18, offset: 230},
							label: "matchBy",
							expr: &ruleRefExpr{
								pos:  position{line: 16, col: 26, offset: 238},
								name: "Any",
							},
						},
					},
				},
			},
		},
		{
			name: "Any",
			pos:  position{line: 20, col: 1, offset: 278},
			expr: &actionExpr{
				pos: position{line: 20, col: 8, offset: 285},
				run: (*parser).callonAny1,
				expr: &zeroOrMoreExpr{
					pos: position{line: 20, col: 8, offset: 285},
					expr: &anyMatcher{
						line: 20, col: 8, offset: 285,
					},
				},
			},
		},
		{
			name: "Expr",
			pos:  position{line: 24, col: 1, offset: 322},
			expr: &choiceExpr{
				pos: position{line: 24, col: 9, offset: 330},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 24, col: 9, offset: 330},
						run: (*parser).callonExpr2,
						expr: &seqExpr{
							pos: position{line: 24, col: 9, offset: 330},
							exprs: []interface{}{
								&ruleRefExpr{
									pos:  position{line: 24, col: 9, offset: 330},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 24, col: 11, offset: 332},
									label: "e1",
									expr: &ruleRefExpr{
										pos:  position{line: 24, col: 14, offset: 335},
										name: "AndExpr",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 24, col: 22, offset: 343},
									name: "_",
								},
								&ruleRefExpr{
									pos:  position{line: 24, col: 24, offset: 345},
									name: "OrOperator",
								},
								&ruleRefExpr{
									pos:  position{line: 24, col: 35, offset: 356},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 24, col: 37, offset: 358},
									label: "e2",
									expr: &ruleRefExpr{
										pos:  position{line: 24, col: 40, offset: 361},
										name: "Expr",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 24, col: 45, offset: 366},
									name: "_",
								},
							},
						},
					},
					&actionExpr{
						pos: position{line: 26, col: 5, offset: 422},
						run: (*parser).callonExpr13,
						expr: &seqExpr{
							pos: position{line: 26, col: 5, offset: 422},
							exprs: []interface{}{
								&ruleRefExpr{
									pos:  position{line: 26, col: 5, offset: 422},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 26, col: 7, offset: 424},
									label: "e",
									expr: &ruleRefExpr{
										pos:  position{line: 26, col: 9, offset: 426},
										name: "AndExpr",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 26, col: 17, offset: 434},
									name: "_",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "AndExpr",
			pos:  position{line: 30, col: 1, offset: 464},
			expr: &choiceExpr{
				pos: position{line: 30, col: 12, offset: 475},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 30, col: 12, offset: 475},
						run: (*parser).callonAndExpr2,
						expr: &seqExpr{
							pos: position{line: 30, col: 12, offset: 475},
							exprs: []interface{}{
								&ruleRefExpr{
									pos:  position{line: 30, col: 12, offset: 475},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 30, col: 14, offset: 477},
									label: "e1",
									expr: &ruleRefExpr{
										pos:  position{line: 30, col: 17, offset: 480},
										name: "SubExpr",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 30, col: 25, offset: 488},
									name: "_",
								},
								&ruleRefExpr{
									pos:  position{line: 30, col: 27, offset: 490},
									name: "AndOperator",
								},
								&ruleRefExpr{
									pos:  position{line: 30, col: 39, offset: 502},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 30, col: 41, offset: 504},
									label: "e2",
									expr: &ruleRefExpr{
										pos:  position{line: 30, col: 44, offset: 507},
										name: "AndExpr",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 30, col: 52, offset: 515},
									name: "_",
								},
							},
						},
					},
					&actionExpr{
						pos: position{line: 32, col: 5, offset: 572},
						run: (*parser).callonAndExpr13,
						expr: &seqExpr{
							pos: position{line: 32, col: 5, offset: 572},
							exprs: []interface{}{
								&ruleRefExpr{
									pos:  position{line: 32, col: 5, offset: 572},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 32, col: 7, offset: 574},
									label: "e",
									expr: &ruleRefExpr{
										pos:  position{line: 32, col: 9, offset: 576},
										name: "SubExpr",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 32, col: 17, offset: 584},
									name: "_",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "SubExpr",
			pos:  position{line: 36, col: 1, offset: 614},
			expr: &choiceExpr{
				pos: position{line: 36, col: 12, offset: 625},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 36, col: 12, offset: 625},
						run: (*parser).callonSubExpr2,
						expr: &seqExpr{
							pos: position{line: 36, col: 12, offset: 625},
							exprs: []interface{}{
								&labeledExpr{
									pos:   position{line: 36, col: 12, offset: 625},
									label: "streamID",
									expr: &ruleRefExpr{
										pos:  position{line: 36, col: 21, offset: 634},
										name: "StreamID",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 36, col: 30, offset: 643},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 36, col: 32, offset: 645},
									label: "op",
									expr: &ruleRefExpr{
										pos:  position{line: 36, col: 35, offset: 648},
										name: "RelationalOperator",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 36, col: 54, offset: 667},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 36, col: 56, offset: 669},
									label: "thresholdValue",
									expr: &ruleRefExpr{
										pos:  position{line: 36, col: 71, offset: 684},
										name: "Float64",
									},
								},
								&ruleRefExpr{
									pos:  position{line: 36, col: 79, offset: 692},
									name: "_",
								},
								&labeledExpr{
									pos:   position{line: 36, col: 81, offset: 694},
									label: "times",
									expr: &zeroOrOneExpr{
										pos: position{line: 36, col: 87, offset: 700},
										expr: &ruleRefExpr{
											pos:  position{line: 36, col: 87, offset: 700},
											name: "Times",
										},
									},
								},
							},
						},
					},
					&actionExpr{
						pos: position{line: 47, col: 5, offset: 913},
						run: (*parser).callonSubExpr16,
						expr: &seqExpr{
							pos: position{line: 47, col: 5, offset: 913},
							exprs: []interface{}{
								&litMatcher{
									pos:        position{line: 47, col: 5, offset: 913},
									val:        "(",
									ignoreCase: false,
								},
								&labeledExpr{
									pos:   position{line: 47, col: 9, offset: 917},
									label: "e",
									expr: &ruleRefExpr{
										pos:  position{line: 47, col: 11, offset: 919},
										name: "Expr",
									},
								},
								&litMatcher{
									pos:        position{line: 47, col: 16, offset: 924},
									val:        ")",
									ignoreCase: false,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Times",
			pos:  position{line: 51, col: 1, offset: 956},
			expr: &actionExpr{
				pos: position{line: 51, col: 10, offset: 965},
				run: (*parser).callonTimes1,
				expr: &seqExpr{
					pos: position{line: 51, col: 10, offset: 965},
					exprs: []interface{}{
						&litMatcher{
							pos:        position{line: 51, col: 10, offset: 965},
							val:        "times",
							ignoreCase: false,
						},
						&ruleRefExpr{
							pos:  position{line: 51, col: 18, offset: 973},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 51, col: 20, offset: 975},
							label: "periods",
							expr: &ruleRefExpr{
								pos:  position{line: 51, col: 28, offset: 983},
								name: "Int",
							},
						},
					},
				},
			},
		},
		{
			name: "Int",
			pos:  position{line: 55, col: 1, offset: 1014},
			expr: &actionExpr{
				pos: position{line: 55, col: 8, offset: 1021},
				run: (*parser).callonInt1,
				expr: &oneOrMoreExpr{
					pos: position{line: 55, col: 8, offset: 1021},
					expr: &charClassMatcher{
						pos:        position{line: 55, col: 8, offset: 1021},
						val:        "[0-9]",
						ranges:     []rune{'0', '9'},
						ignoreCase: false,
						inverted:   false,
					},
				},
			},
		},
		{
			name: "StreamID",
			pos:  position{line: 62, col: 1, offset: 1255},
			expr: &actionExpr{
				pos: position{line: 62, col: 13, offset: 1267},
				run: (*parser).callonStreamID1,
				expr: &choiceExpr{
					pos: position{line: 62, col: 15, offset: 1269},
					alternatives: []interface{}{
						&seqExpr{
							pos: position{line: 62, col: 15, offset: 1269},
							exprs: []interface{}{
								&oneOrMoreExpr{
									pos: position{line: 62, col: 15, offset: 1269},
									expr: &charClassMatcher{
										pos:        position{line: 62, col: 15, offset: 1269},
										val:        "[^ \\n\\t\\r()]",
										chars:      []rune{' ', '\n', '\t', '\r', '(', ')'},
										ignoreCase: false,
										inverted:   true,
									},
								},
								&ruleRefExpr{
									pos:  position{line: 62, col: 29, offset: 1283},
									name: "ParenthesesEnclosure",
								},
							},
						},
						&seqExpr{
							pos: position{line: 63, col: 6, offset: 1309},
							exprs: []interface{}{
								&oneOrMoreExpr{
									pos: position{line: 63, col: 6, offset: 1309},
									expr: &charClassMatcher{
										pos:        position{line: 63, col: 6, offset: 1309},
										val:        "[^ \\n\\t\\r(){}]",
										chars:      []rune{' ', '\n', '\t', '\r', '(', ')', '{', '}'},
										ignoreCase: false,
										inverted:   true,
									},
								},
								&zeroOrOneExpr{
									pos: position{line: 63, col: 22, offset: 1325},
									expr: &seqExpr{
										pos: position{line: 63, col: 23, offset: 1326},
										exprs: []interface{}{
											&litMatcher{
												pos:        position{line: 63, col: 23, offset: 1326},
												val:        "{",
												ignoreCase: false,
											},
											&zeroOrMoreExpr{
												pos: position{line: 63, col: 27, offset: 1330},
												expr: &anyMatcher{
													line: 63, col: 27, offset: 1330,
												},
											},
											&litMatcher{
												pos:        position{line: 63, col: 30, offset: 1333},
												val:        "}",
												ignoreCase: false,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "ParenthesesEnclosure",
			pos:  position{line: 68, col: 1, offset: 1376},
			expr: &seqExpr{
				pos: position{line: 68, col: 25, offset: 1400},
				exprs: []interface{}{
					&litMatcher{
						pos:        position{line: 68, col: 25, offset: 1400},
						val:        "(",
						ignoreCase: false,
					},
					&zeroOrMoreExpr{
						pos: position{line: 68, col: 29, offset: 1404},
						expr: &charClassMatcher{
							pos:        position{line: 68, col: 29, offset: 1404},
							val:        "[^()]",
							chars:      []rune{'(', ')'},
							ignoreCase: false,
							inverted:   true,
						},
					},
					&zeroOrMoreExpr{
						pos: position{line: 68, col: 36, offset: 1411},
						expr: &seqExpr{
							pos: position{line: 68, col: 37, offset: 1412},
							exprs: []interface{}{
								&ruleRefExpr{
									pos:  position{line: 68, col: 37, offset: 1412},
									name: "ParenthesesEnclosure",
								},
								&zeroOrMoreExpr{
									pos: position{line: 68, col: 58, offset: 1433},
									expr: &charClassMatcher{
										pos:        position{line: 68, col: 58, offset: 1433},
										val:        "[^()]",
										chars:      []rune{'(', ')'},
										ignoreCase: false,
										inverted:   true,
									},
								},
							},
						},
					},
					&litMatcher{
						pos:        position{line: 68, col: 67, offset: 1442},
						val:        ")",
						ignoreCase: false,
					},
				},
			},
		},
		{
			name: "OrOperator",
			pos:  position{line: 70, col: 1, offset: 1447},
			expr: &choiceExpr{
				pos: position{line: 70, col: 15, offset: 1461},
				alternatives: []interface{}{
					&litMatcher{
						pos:        position{line: 70, col: 15, offset: 1461},
						val:        "or",
						ignoreCase: false,
					},
					&litMatcher{
						pos:        position{line: 70, col: 22, offset: 1468},
						val:        "||",
						ignoreCase: false,
					},
				},
			},
		},
		{
			name: "AndOperator",
			pos:  position{line: 72, col: 1, offset: 1474},
			expr: &choiceExpr{
				pos: position{line: 72, col: 16, offset: 1489},
				alternatives: []interface{}{
					&litMatcher{
						pos:        position{line: 72, col: 16, offset: 1489},
						val:        "and",
						ignoreCase: false,
					},
					&litMatcher{
						pos:        position{line: 72, col: 24, offset: 1497},
						val:        "&&",
						ignoreCase: false,
					},
				},
			},
		},
		{
			name: "RelationalOperator",
			pos:  position{line: 74, col: 1, offset: 1503},
			expr: &choiceExpr{
				pos: position{line: 75, col: 5, offset: 1529},
				alternatives: []interface{}{
					&actionExpr{
						pos: position{line: 75, col: 5, offset: 1529},
						run: (*parser).callonRelationalOperator2,
						expr: &litMatcher{
							pos:        position{line: 75, col: 5, offset: 1529},
							val:        "lte",
							ignoreCase: false,
						},
					},
					&actionExpr{
						pos: position{line: 76, col: 5, offset: 1559},
						run: (*parser).callonRelationalOperator4,
						expr: &litMatcher{
							pos:        position{line: 76, col: 5, offset: 1559},
							val:        "<=",
							ignoreCase: false,
						},
					},
					&actionExpr{
						pos: position{line: 77, col: 5, offset: 1588},
						run: (*parser).callonRelationalOperator6,
						expr: &litMatcher{
							pos:        position{line: 77, col: 5, offset: 1588},
							val:        "gte",
							ignoreCase: false,
						},
					},
					&actionExpr{
						pos: position{line: 78, col: 5, offset: 1618},
						run: (*parser).callonRelationalOperator8,
						expr: &litMatcher{
							pos:        position{line: 78, col: 5, offset: 1618},
							val:        ">=",
							ignoreCase: false,
						},
					},
					&actionExpr{
						pos: position{line: 79, col: 5, offset: 1647},
						run: (*parser).callonRelationalOperator10,
						expr: &litMatcher{
							pos:        position{line: 79, col: 5, offset: 1647},
							val:        "lt",
							ignoreCase: false,
						},
					},
					&actionExpr{
						pos: position{line: 80, col: 5, offset: 1675},
						run: (*parser).callonRelationalOperator12,
						expr: &litMatcher{
							pos:        position{line: 80, col: 5, offset: 1675},
							val:        "<",
							ignoreCase: false,
						},
					},
					&actionExpr{
						pos: position{line: 81, col: 5, offset: 1702},
						run: (*parser).callonRelationalOperator14,
						expr: &litMatcher{
							pos:        position{line: 81, col: 5, offset: 1702},
							val:        "gt",
							ignoreCase: false,
						},
					},
					&actionExpr{
						pos: position{line: 82, col: 5, offset: 1730},
						run: (*parser).callonRelationalOperator16,
						expr: &litMatcher{
							pos:        position{line: 82, col: 5, offset: 1730},
							val:        ">",
							ignoreCase: false,
						},
					},
				},
			},
		},
		{
			name: "Float64",
			pos:  position{line: 84, col: 1, offset: 1754},
			expr: &actionExpr{
				pos: position{line: 84, col: 12, offset: 1765},
				run: (*parser).callonFloat641,
				expr: &seqExpr{
					pos: position{line: 84, col: 12, offset: 1765},
					exprs: []interface{}{
						&zeroOrOneExpr{
							pos: position{line: 84, col: 12, offset: 1765},
							expr: &seqExpr{
								pos: position{line: 84, col: 13, offset: 1766},
								exprs: []interface{}{
									&litMatcher{
										pos:        position{line: 84, col: 13, offset: 1766},
										val:        "-",
										ignoreCase: false,
									},
									&litMatcher{
										pos:        position{line: 84, col: 17, offset: 1770},
										val:        "+",
										ignoreCase: false,
									},
								},
							},
						},
						&zeroOrMoreExpr{
							pos: position{line: 84, col: 23, offset: 1776},
							expr: &charClassMatcher{
								pos:        position{line: 84, col: 23, offset: 1776},
								val:        "[0-9]",
								ranges:     []rune{'0', '9'},
								ignoreCase: false,
								inverted:   false,
							},
						},
						&zeroOrOneExpr{
							pos: position{line: 84, col: 30, offset: 1783},
							expr: &seqExpr{
								pos: position{line: 84, col: 31, offset: 1784},
								exprs: []interface{}{
									&litMatcher{
										pos:        position{line: 84, col: 31, offset: 1784},
										val:        ".",
										ignoreCase: false,
									},
									&oneOrMoreExpr{
										pos: position{line: 84, col: 35, offset: 1788},
										expr: &charClassMatcher{
											pos:        position{line: 84, col: 35, offset: 1788},
											val:        "[0-9]",
											ranges:     []rune{'0', '9'},
											ignoreCase: false,
											inverted:   false,
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "_",
			displayName: "\"whitespace\"",
			pos:         position{line: 88, col: 1, offset: 1850},
			expr: &zeroOrMoreExpr{
				pos: position{line: 88, col: 19, offset: 1868},
				expr: &charClassMatcher{
					pos:        position{line: 88, col: 19, offset: 1868},
					val:        "[ \\n\\t\\r]",
					chars:      []rune{' ', '\n', '\t', '\r'},
					ignoreCase: false,
					inverted:   false,
				},
			},
		},
		{
			name: "EOF",
			pos:  position{line: 89, col: 1, offset: 1879},
			expr: &notExpr{
				pos: position{line: 89, col: 8, offset: 1886},
				expr: &anyMatcher{
					line: 89, col: 9, offset: 1887,
				},
			},
		},
	},
}

func (c *current) onAlarmDefinition1(e, mb interface{}) (interface{}, error) {
	var matchBy string
	if mb != nil {
		matchBy = mb.(string)
	}
	return alarmDefinition{e.(Node), matchBy}, nil
}

func (p *parser) callonAlarmDefinition1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onAlarmDefinition1(stack["e"], stack["mb"])
}

func (c *current) onMatchBy1(matchBy interface{}) (interface{}, error) {
	return matchBy.(string), nil
}

func (p *parser) callonMatchBy1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onMatchBy1(stack["matchBy"])
}

func (c *current) onAny1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonAny1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onAny1()
}

func (c *current) onExpr2(e1, e2 interface{}) (interface{}, error) {
	return BinNode{OR, e1.(Node), e2.(Node)}, nil
}

func (p *parser) callonExpr2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onExpr2(stack["e1"], stack["e2"])
}

func (c *current) onExpr13(e interface{}) (interface{}, error) {
	return e.(Node), nil
}

func (p *parser) callonExpr13() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onExpr13(stack["e"])
}

func (c *current) onAndExpr2(e1, e2 interface{}) (interface{}, error) {
	return BinNode{AND, e1.(Node), e2.(Node)}, nil
}

func (p *parser) callonAndExpr2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onAndExpr2(stack["e1"], stack["e2"])
}

func (c *current) onAndExpr13(e interface{}) (interface{}, error) {
	return e.(Node), nil
}

func (p *parser) callonAndExpr13() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onAndExpr13(stack["e"])
}

func (c *current) onSubExpr2(streamID, op, thresholdValue, times interface{}) (interface{}, error) {
	periods := 1
	if times != nil {
		periods = times.(int)
	}
	return LeafNode{
		dagger.StreamID(streamID.(string)),
		op.(RelationalOperator),
		thresholdValue.(float64),
		periods,
	}, nil
}

func (p *parser) callonSubExpr2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onSubExpr2(stack["streamID"], stack["op"], stack["thresholdValue"], stack["times"])
}

func (c *current) onSubExpr16(e interface{}) (interface{}, error) {
	return e.(Node), nil
}

func (p *parser) callonSubExpr16() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onSubExpr16(stack["e"])
}

func (c *current) onTimes1(periods interface{}) (interface{}, error) {
	return periods, nil
}

func (p *parser) callonTimes1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onTimes1(stack["periods"])
}

func (c *current) onInt1() (interface{}, error) {
	return strconv.Atoi(string(c.text))
}

func (p *parser) callonInt1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onInt1()
}

func (c *current) onStreamID1() (interface{}, error) {
	return string(c.text), nil
}

func (p *parser) callonStreamID1() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onStreamID1()
}

func (c *current) onRelationalOperator2() (interface{}, error) {
	return LTE, nil
}

func (p *parser) callonRelationalOperator2() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator2()
}

func (c *current) onRelationalOperator4() (interface{}, error) {
	return LTE, nil
}

func (p *parser) callonRelationalOperator4() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator4()
}

func (c *current) onRelationalOperator6() (interface{}, error) {
	return GTE, nil
}

func (p *parser) callonRelationalOperator6() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator6()
}

func (c *current) onRelationalOperator8() (interface{}, error) {
	return GTE, nil
}

func (p *parser) callonRelationalOperator8() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator8()
}

func (c *current) onRelationalOperator10() (interface{}, error) {
	return LT, nil
}

func (p *parser) callonRelationalOperator10() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator10()
}

func (c *current) onRelationalOperator12() (interface{}, error) {
	return LT, nil
}

func (p *parser) callonRelationalOperator12() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator12()
}

func (c *current) onRelationalOperator14() (interface{}, error) {
	return GT, nil
}

func (p *parser) callonRelationalOperator14() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator14()
}

func (c *current) onRelationalOperator16() (interface{}, error) {
	return GT, nil
}

func (p *parser) callonRelationalOperator16() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onRelationalOperator16()
}

func (c *current) onFloat641() (interface{}, error) {
	return strconv.ParseFloat(string(c.text), 64)
}

func (p *parser) callonFloat641() (interface{}, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onFloat641()
}

var (
	// errNoRule is returned when the grammar to parse has no rule.
	errNoRule = errors.New("grammar has no rule")

	// errInvalidEncoding is returned when the source is not properly
	// utf8-encoded.
	errInvalidEncoding = errors.New("invalid encoding")

	// errNoMatch is returned if no match could be found.
	errNoMatch = errors.New("no match found")
)

// Option is a function that can set an option on the parser. It returns
// the previous setting as an Option.
type Option func(*parser) Option

// Debug creates an Option to set the debug flag to b. When set to true,
// debugging information is printed to stdout while parsing.
//
// The default is false.
func Debug(b bool) Option {
	return func(p *parser) Option {
		old := p.debug
		p.debug = b
		return Debug(old)
	}
}

// Memoize creates an Option to set the memoize flag to b. When set to true,
// the parser will cache all results so each expression is evaluated only
// once. This guarantees linear parsing time even for pathological cases,
// at the expense of more memory and slower times for typical cases.
//
// The default is false.
func Memoize(b bool) Option {
	return func(p *parser) Option {
		old := p.memoize
		p.memoize = b
		return Memoize(old)
	}
}

// Recover creates an Option to set the recover flag to b. When set to
// true, this causes the parser to recover from panics and convert it
// to an error. Setting it to false can be useful while debugging to
// access the full stack trace.
//
// The default is true.
func Recover(b bool) Option {
	return func(p *parser) Option {
		old := p.recover
		p.recover = b
		return Recover(old)
	}
}

// ParseFile parses the file identified by filename.
func ParseFile(filename string, opts ...Option) (interface{}, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseReader(filename, f, opts...)
}

// ParseReader parses the data from r using filename as information in the
// error messages.
func ParseReader(filename string, r io.Reader, opts ...Option) (interface{}, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return Parse(filename, b, opts...)
}

// Parse parses the data from b using filename as information in the
// error messages.
func Parse(filename string, b []byte, opts ...Option) (interface{}, error) {
	return newParser(filename, b, opts...).parse(g)
}

// position records a position in the text.
type position struct {
	line, col, offset int
}

func (p position) String() string {
	return fmt.Sprintf("%d:%d [%d]", p.line, p.col, p.offset)
}

// savepoint stores all state required to go back to this point in the
// parser.
type savepoint struct {
	position
	rn rune
	w  int
}

type current struct {
	pos  position // start position of the match
	text []byte   // raw text of the match
}

// the AST types...

type grammar struct {
	pos   position
	rules []*rule
}

type rule struct {
	pos         position
	name        string
	displayName string
	expr        interface{}
}

type choiceExpr struct {
	pos          position
	alternatives []interface{}
}

type actionExpr struct {
	pos  position
	expr interface{}
	run  func(*parser) (interface{}, error)
}

type seqExpr struct {
	pos   position
	exprs []interface{}
}

type labeledExpr struct {
	pos   position
	label string
	expr  interface{}
}

type expr struct {
	pos  position
	expr interface{}
}

type andExpr expr
type notExpr expr
type zeroOrOneExpr expr
type zeroOrMoreExpr expr
type oneOrMoreExpr expr

type ruleRefExpr struct {
	pos  position
	name string
}

type andCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type notCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type litMatcher struct {
	pos        position
	val        string
	ignoreCase bool
}

type charClassMatcher struct {
	pos        position
	val        string
	chars      []rune
	ranges     []rune
	classes    []*unicode.RangeTable
	ignoreCase bool
	inverted   bool
}

type anyMatcher position

// errList cumulates the errors found by the parser.
type errList []error

func (e *errList) add(err error) {
	*e = append(*e, err)
}

func (e errList) err() error {
	if len(e) == 0 {
		return nil
	}
	e.dedupe()
	return e
}

func (e *errList) dedupe() {
	var cleaned []error
	set := make(map[string]bool)
	for _, err := range *e {
		if msg := err.Error(); !set[msg] {
			set[msg] = true
			cleaned = append(cleaned, err)
		}
	}
	*e = cleaned
}

func (e errList) Error() string {
	switch len(e) {
	case 0:
		return ""
	case 1:
		return e[0].Error()
	default:
		var buf bytes.Buffer

		for i, err := range e {
			if i > 0 {
				buf.WriteRune('\n')
			}
			buf.WriteString(err.Error())
		}
		return buf.String()
	}
}

// parserError wraps an error with a prefix indicating the rule in which
// the error occurred. The original error is stored in the Inner field.
type parserError struct {
	Inner  error
	pos    position
	prefix string
}

// Error returns the error message.
func (p *parserError) Error() string {
	return p.prefix + ": " + p.Inner.Error()
}

// newParser creates a parser with the specified input source and options.
func newParser(filename string, b []byte, opts ...Option) *parser {
	p := &parser{
		filename: filename,
		errs:     new(errList),
		data:     b,
		pt:       savepoint{position: position{line: 1}},
		recover:  true,
	}
	p.setOptions(opts)
	return p
}

// setOptions applies the options to the parser.
func (p *parser) setOptions(opts []Option) {
	for _, opt := range opts {
		opt(p)
	}
}

type resultRecord struct {
	v   interface{}
	b   bool
	end savepoint
}

type parser struct {
	filename string
	pt       savepoint
	cur      current

	data []byte
	errs *errList

	recover bool
	debug   bool
	depth   int

	memoize bool
	// memoization table for the packrat algorithm:
	// map[offset in source] map[expression or rule] {value, match}
	memo map[int]map[interface{}]resultRecord

	// rules table, maps the rule identifier to the rule node
	rules map[string]*rule
	// variables stack, map of label to value
	vstack []map[string]interface{}
	// rule stack, allows identification of the current rule in errors
	rstack []*rule

	// stats
	exprCnt int
}

// push a variable set on the vstack.
func (p *parser) pushV() {
	if cap(p.vstack) == len(p.vstack) {
		// create new empty slot in the stack
		p.vstack = append(p.vstack, nil)
	} else {
		// slice to 1 more
		p.vstack = p.vstack[:len(p.vstack)+1]
	}

	// get the last args set
	m := p.vstack[len(p.vstack)-1]
	if m != nil && len(m) == 0 {
		// empty map, all good
		return
	}

	m = make(map[string]interface{})
	p.vstack[len(p.vstack)-1] = m
}

// pop a variable set from the vstack.
func (p *parser) popV() {
	// if the map is not empty, clear it
	m := p.vstack[len(p.vstack)-1]
	if len(m) > 0 {
		// GC that map
		p.vstack[len(p.vstack)-1] = nil
	}
	p.vstack = p.vstack[:len(p.vstack)-1]
}

func (p *parser) print(prefix, s string) string {
	if !p.debug {
		return s
	}

	fmt.Printf("%s %d:%d:%d: %s [%#U]\n",
		prefix, p.pt.line, p.pt.col, p.pt.offset, s, p.pt.rn)
	return s
}

func (p *parser) in(s string) string {
	p.depth++
	return p.print(strings.Repeat(" ", p.depth)+">", s)
}

func (p *parser) out(s string) string {
	p.depth--
	return p.print(strings.Repeat(" ", p.depth)+"<", s)
}

func (p *parser) addErr(err error) {
	p.addErrAt(err, p.pt.position)
}

func (p *parser) addErrAt(err error, pos position) {
	var buf bytes.Buffer
	if p.filename != "" {
		buf.WriteString(p.filename)
	}
	if buf.Len() > 0 {
		buf.WriteString(":")
	}
	buf.WriteString(fmt.Sprintf("%d:%d (%d)", pos.line, pos.col, pos.offset))
	if len(p.rstack) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(": ")
		}
		rule := p.rstack[len(p.rstack)-1]
		if rule.displayName != "" {
			buf.WriteString("rule " + rule.displayName)
		} else {
			buf.WriteString("rule " + rule.name)
		}
	}
	pe := &parserError{Inner: err, prefix: buf.String()}
	p.errs.add(pe)
}

// read advances the parser to the next rune.
func (p *parser) read() {
	p.pt.offset += p.pt.w
	rn, n := utf8.DecodeRune(p.data[p.pt.offset:])
	p.pt.rn = rn
	p.pt.w = n
	p.pt.col++
	if rn == '\n' {
		p.pt.line++
		p.pt.col = 0
	}

	if rn == utf8.RuneError {
		if n > 0 {
			p.addErr(errInvalidEncoding)
		}
	}
}

// restore parser position to the savepoint pt.
func (p *parser) restore(pt savepoint) {
	if p.debug {
		defer p.out(p.in("restore"))
	}
	if pt.offset == p.pt.offset {
		return
	}
	p.pt = pt
}

// get the slice of bytes from the savepoint start to the current position.
func (p *parser) sliceFrom(start savepoint) []byte {
	return p.data[start.position.offset:p.pt.position.offset]
}

func (p *parser) getMemoized(node interface{}) (resultRecord, bool) {
	if len(p.memo) == 0 {
		return resultRecord{}, false
	}
	m := p.memo[p.pt.offset]
	if len(m) == 0 {
		return resultRecord{}, false
	}
	res, ok := m[node]
	return res, ok
}

func (p *parser) setMemoized(pt savepoint, node interface{}, record resultRecord) {
	if p.memo == nil {
		p.memo = make(map[int]map[interface{}]resultRecord)
	}
	m := p.memo[pt.offset]
	if m == nil {
		m = make(map[interface{}]resultRecord)
		p.memo[pt.offset] = m
	}
	m[node] = record
}

func (p *parser) buildRulesTable(g *grammar) {
	p.rules = make(map[string]*rule, len(g.rules))
	for _, r := range g.rules {
		p.rules[r.name] = r
	}
}

func (p *parser) parse(g *grammar) (val interface{}, err error) {
	if len(g.rules) == 0 {
		p.addErr(errNoRule)
		return nil, p.errs.err()
	}

	// TODO : not super critical but this could be generated
	p.buildRulesTable(g)

	if p.recover {
		// panic can be used in action code to stop parsing immediately
		// and return the panic as an error.
		defer func() {
			if e := recover(); e != nil {
				if p.debug {
					defer p.out(p.in("panic handler"))
				}
				val = nil
				switch e := e.(type) {
				case error:
					p.addErr(e)
				default:
					p.addErr(fmt.Errorf("%v", e))
				}
				err = p.errs.err()
			}
		}()
	}

	// start rule is rule [0]
	p.read() // advance to first rune
	val, ok := p.parseRule(g.rules[0])
	if !ok {
		if len(*p.errs) == 0 {
			// make sure this doesn't go out silently
			p.addErr(errNoMatch)
		}
		return nil, p.errs.err()
	}
	return val, p.errs.err()
}

func (p *parser) parseRule(rule *rule) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRule " + rule.name))
	}

	if p.memoize {
		res, ok := p.getMemoized(rule)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
	}

	start := p.pt
	p.rstack = append(p.rstack, rule)
	p.pushV()
	val, ok := p.parseExpr(rule.expr)
	p.popV()
	p.rstack = p.rstack[:len(p.rstack)-1]
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth)+"MATCH", string(p.sliceFrom(start)))
	}

	if p.memoize {
		p.setMemoized(start, rule, resultRecord{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseExpr(expr interface{}) (interface{}, bool) {
	var pt savepoint
	var ok bool

	if p.memoize {
		res, ok := p.getMemoized(expr)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
		pt = p.pt
	}

	p.exprCnt++
	var val interface{}
	switch expr := expr.(type) {
	case *actionExpr:
		val, ok = p.parseActionExpr(expr)
	case *andCodeExpr:
		val, ok = p.parseAndCodeExpr(expr)
	case *andExpr:
		val, ok = p.parseAndExpr(expr)
	case *anyMatcher:
		val, ok = p.parseAnyMatcher(expr)
	case *charClassMatcher:
		val, ok = p.parseCharClassMatcher(expr)
	case *choiceExpr:
		val, ok = p.parseChoiceExpr(expr)
	case *labeledExpr:
		val, ok = p.parseLabeledExpr(expr)
	case *litMatcher:
		val, ok = p.parseLitMatcher(expr)
	case *notCodeExpr:
		val, ok = p.parseNotCodeExpr(expr)
	case *notExpr:
		val, ok = p.parseNotExpr(expr)
	case *oneOrMoreExpr:
		val, ok = p.parseOneOrMoreExpr(expr)
	case *ruleRefExpr:
		val, ok = p.parseRuleRefExpr(expr)
	case *seqExpr:
		val, ok = p.parseSeqExpr(expr)
	case *zeroOrMoreExpr:
		val, ok = p.parseZeroOrMoreExpr(expr)
	case *zeroOrOneExpr:
		val, ok = p.parseZeroOrOneExpr(expr)
	default:
		panic(fmt.Sprintf("unknown expression type %T", expr))
	}
	if p.memoize {
		p.setMemoized(pt, expr, resultRecord{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseActionExpr(act *actionExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseActionExpr"))
	}

	start := p.pt
	val, ok := p.parseExpr(act.expr)
	if ok {
		p.cur.pos = start.position
		p.cur.text = p.sliceFrom(start)
		actVal, err := act.run(p)
		if err != nil {
			p.addErrAt(err, start.position)
		}
		val = actVal
	}
	if ok && p.debug {
		p.print(strings.Repeat(" ", p.depth)+"MATCH", string(p.sliceFrom(start)))
	}
	return val, ok
}

func (p *parser) parseAndCodeExpr(and *andCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndCodeExpr"))
	}

	ok, err := and.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, ok
}

func (p *parser) parseAndExpr(and *andExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAndExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(and.expr)
	p.popV()
	p.restore(pt)
	return nil, ok
}

func (p *parser) parseAnyMatcher(any *anyMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseAnyMatcher"))
	}

	if p.pt.rn != utf8.RuneError {
		start := p.pt
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseCharClassMatcher(chr *charClassMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseCharClassMatcher"))
	}

	cur := p.pt.rn
	// can't match EOF
	if cur == utf8.RuneError {
		return nil, false
	}
	start := p.pt
	if chr.ignoreCase {
		cur = unicode.ToLower(cur)
	}

	// try to match in the list of available chars
	for _, rn := range chr.chars {
		if rn == cur {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of ranges
	for i := 0; i < len(chr.ranges); i += 2 {
		if cur >= chr.ranges[i] && cur <= chr.ranges[i+1] {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of Unicode classes
	for _, cl := range chr.classes {
		if unicode.Is(cl, cur) {
			if chr.inverted {
				return nil, false
			}
			p.read()
			return p.sliceFrom(start), true
		}
	}

	if chr.inverted {
		p.read()
		return p.sliceFrom(start), true
	}
	return nil, false
}

func (p *parser) parseChoiceExpr(ch *choiceExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseChoiceExpr"))
	}

	for _, alt := range ch.alternatives {
		p.pushV()
		val, ok := p.parseExpr(alt)
		p.popV()
		if ok {
			return val, ok
		}
	}
	return nil, false
}

func (p *parser) parseLabeledExpr(lab *labeledExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLabeledExpr"))
	}

	p.pushV()
	val, ok := p.parseExpr(lab.expr)
	p.popV()
	if ok && lab.label != "" {
		m := p.vstack[len(p.vstack)-1]
		m[lab.label] = val
	}
	return val, ok
}

func (p *parser) parseLitMatcher(lit *litMatcher) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseLitMatcher"))
	}

	start := p.pt
	for _, want := range lit.val {
		cur := p.pt.rn
		if lit.ignoreCase {
			cur = unicode.ToLower(cur)
		}
		if cur != want {
			p.restore(start)
			return nil, false
		}
		p.read()
	}
	return p.sliceFrom(start), true
}

func (p *parser) parseNotCodeExpr(not *notCodeExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotCodeExpr"))
	}

	ok, err := not.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, !ok
}

func (p *parser) parseNotExpr(not *notExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseNotExpr"))
	}

	pt := p.pt
	p.pushV()
	_, ok := p.parseExpr(not.expr)
	p.popV()
	p.restore(pt)
	return nil, !ok
}

func (p *parser) parseOneOrMoreExpr(expr *oneOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseOneOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			if len(vals) == 0 {
				// did not match once, no match
				return nil, false
			}
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseRuleRefExpr(ref *ruleRefExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseRuleRefExpr " + ref.name))
	}

	if ref.name == "" {
		panic(fmt.Sprintf("%s: invalid rule: missing name", ref.pos))
	}

	rule := p.rules[ref.name]
	if rule == nil {
		p.addErr(fmt.Errorf("undefined rule: %s", ref.name))
		return nil, false
	}
	return p.parseRule(rule)
}

func (p *parser) parseSeqExpr(seq *seqExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseSeqExpr"))
	}

	var vals []interface{}

	pt := p.pt
	for _, expr := range seq.exprs {
		val, ok := p.parseExpr(expr)
		if !ok {
			p.restore(pt)
			return nil, false
		}
		vals = append(vals, val)
	}
	return vals, true
}

func (p *parser) parseZeroOrMoreExpr(expr *zeroOrMoreExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrMoreExpr"))
	}

	var vals []interface{}

	for {
		p.pushV()
		val, ok := p.parseExpr(expr.expr)
		p.popV()
		if !ok {
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseZeroOrOneExpr(expr *zeroOrOneExpr) (interface{}, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrOneExpr"))
	}

	p.pushV()
	val, _ := p.parseExpr(expr.expr)
	p.popV()
	// whether it matched or not, consider it a match
	return val, true
}

func rangeTable(class string) *unicode.RangeTable {
	if rt, ok := unicode.Categories[class]; ok {
		return rt
	}
	if rt, ok := unicode.Properties[class]; ok {
		return rt
	}
	if rt, ok := unicode.Scripts[class]; ok {
		return rt
	}

	// cannot happen
	panic(fmt.Sprintf("invalid Unicode class: %s", class))
}
