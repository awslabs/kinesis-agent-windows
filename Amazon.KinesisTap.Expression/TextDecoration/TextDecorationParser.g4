parser grammar TextDecorationParser;
options { tokenVocab=TextDecorationLexer; }

textDecoration
    :   (variable|TEXT)*
    ;

variable
    :   '{' expression '}'
    ;

expression
    :   identifierExpression    
    |   constantExpression     
    |   invocationExpression   
    ;

constantExpression
    :   STRING
    |   NUMBER
    |   'true'
    |   'false'
    |   'null'
    ;

identifierExpression
    :   IDENTIFIER
	|   QUOTED_IDENTIFIER
    ;

invocationExpression
    :   IDENTIFIER '(' arguments? ')'
    ;

arguments
    :   expression (COMMA expression)*
    ;


