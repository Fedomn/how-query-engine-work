package sql

import "fmt"

type TokenType int

const (
	// literal
	LiteralLong TokenType = iota + 1
	LiteralDouble
	LiteralString
	LiteralIdentifier

	// keyword common
	KeywordSchema
	KeywordDatabase
	KeywordTable
	KeywordColumn
	KeywordView
	KeywordIndex
	KeywordTrigger
	KeywordProcedure
	KeywordTablespace
	KeywordFunction
	KeywordSequence
	KeywordCursor
	KeywordFrom
	KeywordTo
	KeywordOf
	KeywordIf
	KeywordOn
	KeywordFor
	KeywordWhile
	KeywordDo
	KeywordNo
	KeywordBy
	KeywordWith
	KeywordWithout
	KeywordTrue
	KeywordFalse
	KeywordTemporary
	KeywordTemp
	KeywordComment

	// keyword create
	KeywordCreate
	KeywordReplace
	KeywordBefore
	KeywordAfter
	KeywordInstead
	KeywordEach
	KeywordRow
	KeywordStatement
	KeywordExecute
	KeywordBitmap
	KeywordNosort
	KeywordReverse
	KeywordCompile

	// keyword alter
	KeywordAlter
	KeywordAdd
	KeywordModify
	KeywordRename
	KeywordEnable
	KeywordDisable
	KeywordValidate
	KeywordUser
	KeywordIdentified

	// keyword truncate
	KeywordTruncate

	// keyword drop
	KeywordDrop
	KeywordCascade

	// keyword insert
	KeywordInsert
	KeywordInto
	KeywordValues

	// keyword update
	KeywordUpdate
	KeywordSet

	// keyword delete
	KeywordDelete

	// keyword select
	KeywordSelect
	KeywordDistinct
	KeywordAs
	KeywordCase
	KeywordWhen
	KeywordElse
	KeywordThen
	KeywordEnd
	KeywordLeft
	KeywordRight
	KeywordFull
	KeywordInner
	KeywordOuter
	KeywordCross
	KeywordJoin
	KeywordUse
	KeywordUsing
	KeywordNatural
	KeywordWhere
	KeywordOrder
	KeywordAsc
	KeywordDesc
	KeywordGroup
	KeywordHaving
	KeywordUnion

	// keyword others
	KeywordDeclare
	KeywordGrant
	KeywordFetch
	KeywordRevoke
	KeywordClose
	KeywordCast
	KeywordNew
	KeywordEscape
	KeywordLock
	KeywordSome
	KeywordLeave
	KeywordIterate
	KeywordRepeat
	KeywordUntil
	KeywordOpen
	KeywordOut
	KeywordInout
	KeywordOver
	KeywordAdvise
	KeywordSiblings
	KeywordLoop
	KeywordExplain
	KeywordDefault
	KeywordExcept
	KeywordIntersect
	KeywordMinus
	KeywordPassword
	KeywordLocal
	KeywordGlobal
	KeywordStorage
	KeywordData
	KeywordCoalesce

	// keyword types
	KeywordChar
	KeywordCharacter
	KeywordVarying
	KeywordVarchar
	KeywordVarchar2
	KeywordInteger
	KeywordInt
	KeywordSmallint
	KeywordDecimal
	KeywordDec
	KeywordNumeric
	KeywordFloat
	KeywordReal
	KeywordDouble
	KeywordPrecision
	KeywordDate
	KeywordTime
	KeywordInterval
	KeywordBoolean
	KeywordBlob

	// keyword conditionals
	KeywordAnd
	KeywordOr
	KeywordXor
	KeywordIs
	KeywordNot
	KeywordNull
	KeywordIn
	KeywordBetween
	KeywordLike
	KeywordAny
	KeywordAll
	KeywordExists

	// keyword functions
	KeywordAvg
	KeywordMax
	KeywordMin
	KeywordSum
	KeywordCount
	KeywordGreatest
	KeywordLeast
	KeywordRound
	KeywordTrunc
	KeywordPosition
	KeywordExtract
	KeywordLength
	KeywordCharLength
	KeywordSubstring
	KeywordSubstr
	KeywordInstr
	KeywordInitCap
	KeywordUpper
	KeywordLower
	KeywordTrim
	KeywordLtrim
	KeywordRtrim
	KeywordBoth
	KeywordLeading
	KeywordTrailing
	KeywordTranslate
	KeywordConvert
	KeywordLpad
	KeywordRpad
	KeywordDecode
	KeywordNvl

	// keyword constraints
	KeywordConstraint
	KeywordUnique
	KeywordPrimary
	KeywordForeign
	KeywordKey
	KeywordCheck
	KeywordReferences

	// symbol
	SymbolLeftRaren
	SymbolRightRaren
	SymbolLeftBrace
	SymbolRightBrace
	SymbolLeftBracket
	SymbolRightBracket
	SymbolSemi
	SymbolComma
	SymbolDot
	SymbolDoubleDot
	SymbolPlus
	SymbolSub
	SymbolStar
	SymbolSlash
	SymbolQuestion
	SymbolEq
	SymbolGt
	SymbolLt
	SymbolBang
	SymbolTilde
	SymbolCaret
	SymbolPercent
	SymbolColon
	SymbolDoubleColon
	SymbolColonEq
	SymbolLtEq
	SymbolGtEq
	SymbolLtEqGt
	SymbolLtGt
	SymbolBangEq
	SymbolBangGt
	SymbolBangLt
	SymbolAmp
	SymbolBar
	SymbolDoubleAmp
	SymbolDoubleBar
	SymbolDoubleLt
	SymbolDoubleGt
	SymbolAt
	SymbolPound
)

var tokenTypes = [...]struct {
	name string
}{
	// literal
	LiteralLong:       {"LiteralLong"},
	LiteralDouble:     {"LiteralDouble"},
	LiteralString:     {"LiteralString"},
	LiteralIdentifier: {"LiteralIdentifier"},

	// keyword common
	KeywordSchema:     {"KeywordSchema"},
	KeywordDatabase:   {"KeywordDatabase"},
	KeywordTable:      {"KeywordTable"},
	KeywordColumn:     {"KeywordColumn"},
	KeywordView:       {"KeywordView"},
	KeywordIndex:      {"KeywordIndex"},
	KeywordTrigger:    {"KeywordTrigger"},
	KeywordProcedure:  {"KeywordProcedure"},
	KeywordTablespace: {"KeywordTablespace"},
	KeywordFunction:   {"KeywordFunction"},
	KeywordSequence:   {"KeywordSequence"},
	KeywordCursor:     {"KeywordCursor"},
	KeywordFrom:       {"KeywordFrom"},
	KeywordTo:         {"KeywordTo"},
	KeywordOf:         {"KeywordOf"},
	KeywordIf:         {"KeywordIf"},
	KeywordOn:         {"KeywordOn"},
	KeywordFor:        {"KeywordFor"},
	KeywordWhile:      {"KeywordWhile"},
	KeywordDo:         {"KeywordDo"},
	KeywordNo:         {"KeywordNo"},
	KeywordBy:         {"KeywordBy"},
	KeywordWith:       {"KeywordWith"},
	KeywordWithout:    {"KeywordWithout"},
	KeywordTrue:       {"KeywordTrue"},
	KeywordFalse:      {"KeywordFalse"},
	KeywordTemporary:  {"KeywordTemporary"},
	KeywordTemp:       {"KeywordTemp"},
	KeywordComment:    {"KeywordComment"},

	// keyword create
	KeywordCreate:    {"KeywordCreate"},
	KeywordReplace:   {"KeywordReplace"},
	KeywordBefore:    {"KeywordBefore"},
	KeywordAfter:     {"KeywordAfter"},
	KeywordInstead:   {"KeywordInstead"},
	KeywordEach:      {"KeywordEach"},
	KeywordRow:       {"KeywordRow"},
	KeywordStatement: {"KeywordStatement"},
	KeywordExecute:   {"KeywordExecute"},
	KeywordBitmap:    {"KeywordBitmap"},
	KeywordNosort:    {"KeywordNosort"},
	KeywordReverse:   {"KeywordReverse"},
	KeywordCompile:   {"KeywordCompile"},

	// keyword alter
	KeywordAlter:      {"KeywordAlter"},
	KeywordAdd:        {"KeywordAdd"},
	KeywordModify:     {"KeywordModify"},
	KeywordRename:     {"KeywordRename"},
	KeywordEnable:     {"KeywordEnable"},
	KeywordDisable:    {"KeywordDisable"},
	KeywordValidate:   {"KeywordValidate"},
	KeywordUser:       {"KeywordUser"},
	KeywordIdentified: {"KeywordIdentified"},

	// keyword truncate
	KeywordTruncate: {"KeywordTruncate"},

	// keyword drop
	KeywordDrop:    {"KeywordDrop"},
	KeywordCascade: {"KeywordCascade"},

	// keyword insert
	KeywordInsert: {"KeywordInsert"},
	KeywordInto:   {"KeywordInto"},
	KeywordValues: {"KeywordValues"},

	// keyword update
	KeywordUpdate: {"KeywordUpdate"},
	KeywordSet:    {"KeywordSet"},

	// keyword delete
	KeywordDelete: {"KeywordDelete"},

	// keyword select
	KeywordSelect:   {"KeywordSelect"},
	KeywordDistinct: {"KeywordDistinct"},
	KeywordAs:       {"KeywordAs"},
	KeywordCase:     {"KeywordCase"},
	KeywordWhen:     {"KeywordWhen"},
	KeywordElse:     {"KeywordElse"},
	KeywordThen:     {"KeywordThen"},
	KeywordEnd:      {"KeywordEnd"},
	KeywordLeft:     {"KeywordLeft"},
	KeywordRight:    {"KeywordRight"},
	KeywordFull:     {"KeywordFull"},
	KeywordInner:    {"KeywordInner"},
	KeywordOuter:    {"KeywordOuter"},
	KeywordCross:    {"KeywordCross"},
	KeywordJoin:     {"KeywordJoin"},
	KeywordUse:      {"KeywordUse"},
	KeywordUsing:    {"KeywordUsing"},
	KeywordNatural:  {"KeywordNatural"},
	KeywordWhere:    {"KeywordWhere"},
	KeywordOrder:    {"KeywordOrder"},
	KeywordAsc:      {"KeywordAsc"},
	KeywordDesc:     {"KeywordDesc"},
	KeywordGroup:    {"KeywordGroup"},
	KeywordHaving:   {"KeywordHaving"},
	KeywordUnion:    {"KeywordUnion"},

	// keyword others
	KeywordDeclare:   {"KeywordDeclare"},
	KeywordGrant:     {"KeywordGrant"},
	KeywordFetch:     {"KeywordFetch"},
	KeywordRevoke:    {"KeywordRevoke"},
	KeywordClose:     {"KeywordClose"},
	KeywordCast:      {"KeywordCast"},
	KeywordNew:       {"KeywordNew"},
	KeywordEscape:    {"KeywordEscape"},
	KeywordLock:      {"KeywordLock"},
	KeywordSome:      {"KeywordSome"},
	KeywordLeave:     {"KeywordLeave"},
	KeywordIterate:   {"KeywordIterate"},
	KeywordRepeat:    {"KeywordRepeat"},
	KeywordUntil:     {"KeywordUntil"},
	KeywordOpen:      {"KeywordOpen"},
	KeywordOut:       {"KeywordOut"},
	KeywordInout:     {"KeywordInout"},
	KeywordOver:      {"KeywordOver"},
	KeywordAdvise:    {"KeywordAdvise"},
	KeywordSiblings:  {"KeywordSiblings"},
	KeywordLoop:      {"KeywordLoop"},
	KeywordExplain:   {"KeywordExplain"},
	KeywordDefault:   {"KeywordDefault"},
	KeywordExcept:    {"KeywordExcept"},
	KeywordIntersect: {"KeywordIntersect"},
	KeywordMinus:     {"KeywordMinus"},
	KeywordPassword:  {"KeywordPassword"},
	KeywordLocal:     {"KeywordLocal"},
	KeywordGlobal:    {"KeywordGlobal"},
	KeywordStorage:   {"KeywordStorage"},
	KeywordData:      {"KeywordData"},
	KeywordCoalesce:  {"KeywordCoalesce"},

	// keyword types
	KeywordChar:      {"KeywordChar"},
	KeywordCharacter: {"KeywordCharacter"},
	KeywordVarying:   {"KeywordVarying"},
	KeywordVarchar:   {"KeywordVarchar"},
	KeywordVarchar2:  {"KeywordVarchar2"},
	KeywordInteger:   {"KeywordInteger"},
	KeywordInt:       {"KeywordInt"},
	KeywordSmallint:  {"KeywordSmallint"},
	KeywordDecimal:   {"KeywordDecimal"},
	KeywordDec:       {"KeywordDec"},
	KeywordNumeric:   {"KeywordNumeric"},
	KeywordFloat:     {"KeywordFloat"},
	KeywordReal:      {"KeywordReal"},
	KeywordDouble:    {"KeywordDouble"},
	KeywordPrecision: {"KeywordPrecision"},
	KeywordDate:      {"KeywordDate"},
	KeywordTime:      {"KeywordTime"},
	KeywordInterval:  {"KeywordInterval"},
	KeywordBoolean:   {"KeywordBoolean"},
	KeywordBlob:      {"KeywordBlob"},

	// keyword conditionals
	KeywordAnd:     {"KeywordAnd"},
	KeywordOr:      {"KeywordOr"},
	KeywordXor:     {"KeywordXor"},
	KeywordIs:      {"KeywordIs"},
	KeywordNot:     {"KeywordNot"},
	KeywordNull:    {"KeywordNull"},
	KeywordIn:      {"KeywordIn"},
	KeywordBetween: {"KeywordBetween"},
	KeywordLike:    {"KeywordLike"},
	KeywordAny:     {"KeywordAny"},
	KeywordAll:     {"KeywordAll"},
	KeywordExists:  {"KeywordExists"},

	// keyword functions
	KeywordAvg:        {"KeywordAvg"},
	KeywordMax:        {"KeywordMax"},
	KeywordMin:        {"KeywordMin"},
	KeywordSum:        {"KeywordSum"},
	KeywordCount:      {"KeywordCount"},
	KeywordGreatest:   {"KeywordGreatest"},
	KeywordLeast:      {"KeywordLeast"},
	KeywordRound:      {"KeywordRound"},
	KeywordTrunc:      {"KeywordTrunc"},
	KeywordPosition:   {"KeywordPosition"},
	KeywordExtract:    {"KeywordExtract"},
	KeywordLength:     {"KeywordLength"},
	KeywordCharLength: {"KeywordCharLength"},
	KeywordSubstring:  {"KeywordSubstring"},
	KeywordSubstr:     {"KeywordSubstr"},
	KeywordInstr:      {"KeywordInstr"},
	KeywordInitCap:    {"KeywordInitCap"},
	KeywordUpper:      {"KeywordUpper"},
	KeywordLower:      {"KeywordLower"},
	KeywordTrim:       {"KeywordTrim"},
	KeywordLtrim:      {"KeywordLtrim"},
	KeywordRtrim:      {"KeywordRtrim"},
	KeywordBoth:       {"KeywordBoth"},
	KeywordLeading:    {"KeywordLeading"},
	KeywordTrailing:   {"KeywordTrailing"},
	KeywordTranslate:  {"KeywordTranslate"},
	KeywordConvert:    {"KeywordConvert"},
	KeywordLpad:       {"KeywordLpad"},
	KeywordRpad:       {"KeywordRpad"},
	KeywordDecode:     {"KeywordDecode"},
	KeywordNvl:        {"KeywordNvl"},

	// keyword constraints
	KeywordConstraint: {"KeywordConstraint"},
	KeywordUnique:     {"KeywordUnique"},
	KeywordPrimary:    {"KeywordPrimary"},
	KeywordForeign:    {"KeywordForeign"},
	KeywordKey:        {"KeywordKey"},
	KeywordCheck:      {"KeywordCheck"},
	KeywordReferences: {"KeywordReferences"},

	// symbol
	SymbolLeftRaren:    {"SymbolLeftRaren"},
	SymbolRightRaren:   {"SymbolRightRaren"},
	SymbolLeftBrace:    {"SymbolLeftBrace"},
	SymbolRightBrace:   {"SymbolRightBrace"},
	SymbolLeftBracket:  {"SymbolLeftBracket"},
	SymbolRightBracket: {"SymbolRightBracket"},
	SymbolSemi:         {"SymbolSemi"},
	SymbolComma:        {"SymbolComma"},
	SymbolDot:          {"SymbolDot"},
	SymbolDoubleDot:    {"SymbolDoubleDot"},
	SymbolPlus:         {"SymbolPlus"},
	SymbolSub:          {"SymbolSub"},
	SymbolStar:         {"SymbolStar"},
	SymbolSlash:        {"SymbolSlash"},
	SymbolQuestion:     {"SymbolQuestion"},
	SymbolEq:           {"SymbolEq"},
	SymbolGt:           {"SymbolGt"},
	SymbolLt:           {"SymbolLt"},
	SymbolBang:         {"SymbolBang"},
	SymbolTilde:        {"SymbolTilde"},
	SymbolCaret:        {"SymbolCaret"},
	SymbolPercent:      {"SymbolPercent"},
	SymbolColon:        {"SymbolColon"},
	SymbolDoubleColon:  {"SymbolDoubleColon"},
	SymbolColonEq:      {"SymbolColonEq"},
	SymbolLtEq:         {"SymbolLtEq"},
	SymbolGtEq:         {"SymbolGtEq"},
	SymbolLtEqGt:       {"SymbolLtEqGt"},
	SymbolLtGt:         {"SymbolLtGt"},
	SymbolBangEq:       {"SymbolBangEq"},
	SymbolBangGt:       {"SymbolBangGt"},
	SymbolBangLt:       {"SymbolBangLt"},
	SymbolAmp:          {"SymbolAmp"},
	SymbolBar:          {"SymbolBar"},
	SymbolDoubleAmp:    {"SymbolDoubleAmp"},
	SymbolDoubleBar:    {"SymbolDoubleBar"},
	SymbolDoubleLt:     {"SymbolDoubleLt"},
	SymbolDoubleGt:     {"SymbolDoubleGt"},
	SymbolAt:           {"SymbolAt"},
	SymbolPound:        {"SymbolPound"},
}

type Token struct {
	Text      string
	Type      TokenType
	EndOffset int
}

func (t Token) String() string {
	return fmt.Sprintf("Token(%s, %d, %d)", t.Text, tokenTypes[t.Type], t.EndOffset)
}
