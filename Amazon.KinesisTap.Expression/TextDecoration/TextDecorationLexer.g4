lexer grammar TextDecorationLexer;

TEXT: ('@{' | TEXT_CHAR+ ) ;

LCURL: '{' -> mode(EXPRESSION) ;

fragment TEXT_CHAR : ~[{] ;

mode EXPRESSION;

TRUE: 'true' ;
FALSE: 'false' ;
NULL: 'null' ;

IDENTIFIER
    : (LETTER | '_' | '$') (LETTER | DIGIT | '_' | ':')*
    ;

QUOTED_IDENTIFIER
	: '$\''  (ESC | ~['\\])* '\''
	;

STRING
    : '\'' (ESC | ~['\\])* '\''	
    ;

NUMBER
    :   '-'? INT '.' [0-9]+ EXP? // 1.35, 1.35E-9, 0.3, -4.5
    |   '-'? INT EXP             // 1e10 -3e4
    |   '-'? INT                 // -3, 45
    ;

RCURL: '}' -> mode(DEFAULT_MODE) ;
LPAREN: '(' ;
RPAREN: ')' ;
COMMA: ',' ;

fragment LETTER  : [a-zA-Z] ;
fragment ESC :   '\\' ['\\nrt] ;
fragment INT :   '0' | [1-9] DIGIT* ; // no leading zeros
fragment EXP :   [Ee] [+\-]? INT ; // \- since - means "range" inside [...]
fragment DIGIT:  [0-9] ;

WS : [ \t\r\n]+ -> skip;