parser grammar ObjectDecorationParser;
options { tokenVocab=ObjectDecorationLexer; }

objectDecoration
    :   keyValuePair (';' keyValuePair)*
    ;

keyValuePair
    :   key '=' value
    ;

key
	:	TEXT
	;

value
    :   (variable | TEXT)*
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
