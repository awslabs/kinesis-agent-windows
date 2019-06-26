# Object Decoration and Text Decoration with Expressions
## Overview
### Object Decoration with Expressions

In order to allow more flexible data extraction, we enhanced the existing ObjectDecoration syntax.

The old syntax:

```
"ObjectDecoration": "attribute1=sometext11{variable11}sometext12{variable12};attribute2={variable2}..."
```
We had a list of key/value pairs separated by ";". Each value can be a concatenation of text and variables.

With the new syntax, we enhanced the variable to allow expressions.

```
"ObjectDecorationEx": "attribute1=sometext11{expression11}sometext12{expression12};attribute2={expression2}..."
```

Most of the old ObjectDecoration declaration should work in the new syntax with the exception that we changed the way to specify the timestamp variable.

In ObjectDecoration, we can use:
```
{timestamp:yyyyMMdd}
```
In ObjectDecorationEx, we should use instead:
```
{format(_timestamp, 'yyyyMMdd')}
```

### Text Decoration with Expressions

TextDecoration allows users to compose log record from data. However, users were not able to generate Json previously because TextDecoration use curly brace to denote variable, e.g., {myVar1}.

In the new TextDecoratiionEx syntax, we use '@{' to escape '{'. In addition, we can user expressions like ObjectDecorationEx.

Now we create generate Json with something like:

```
"TextDecorationEx": "@{ \"var\": \"{upper($myvar1)}\" }"
```

## Details
An expression can be:

* A variable expression.
* A constant expression, e.g., 'hello', 1, 1.21, null, true, false.
* An invocation expression that calls function, such as:
```
regexp_extract('Info: MID 118667291 ICID 197973259 RID 0 To: <jd@acme.com>', 'To: (\\\\S+)', 1) 
```
**Note that because the string in our expression uses \ for escape. Json also uses \ for escape. In the above example, a single \ in regular expression became \\\\.**

Function invocations can be nested, for example:
```
format(date(2018, 11, 28), 'MMddyyyy')
```
### Variables
There are 3 types variable: local, meta and global.

#### Local variable
Local variables start with "$", e.g., $message. It is used to resolve the property of the event object, an entry if the event is an dictionary or an attribute if the event is an JSON. If the local variable contains space or special characters, we can use quoted local variable, for example $'date created'.

#### Meta variable
Meta variables start with "_" and is used to resolve to the metadata of the event. 

All event types support the following meta variables

* _timestamp: The timestamp of the event.
* _record: The raw/text representation of the vent

Log events support the following additional meta variables:
* _filepath
* _filename
* _position
* _linenumber

#### Global variable

Global variables resolve to environment variable, ec2 instance metadata, or ec2tag. For better performance, it is recommended that user use the prefix to limit search scope, such as env:computername, ec2:instanceId and ec2tag:Name.

### Built-in functions

We currently support the following built-in functions:
```
//string functions
int length(string input)
string lower(string input)
string lpad(string input, int size, string padstring)
string ltrim(string input)
string rpad(string input, int size, string padstring)
string rtrim(string input)
string substr(string input, int start)
string substr(string input, int start, int length)
string trim(string input)
string upper(string str)

//regular expression functions
string regexp_extract(string input, string pattern)
string regexp_extract(string input, string pattern, int group)

//date functions
DateTime date(int year, int month, int day)
DateTime date(int year, int month, int day, int hour, int minute, int second)
DateTime date(int year, int month, int day, int hour, int minute, int second, int millisecond)

//conversion functions
int? parse_int(string input)
decimal? parse_decimal(string input)
DateTime? parse_date(string input, string format)
string format(object o, string format)

//coalesce functions
object coalesce(object obj1, object obj2)
object coalesce(object obj1, object obj2, object obj3)
object coalesce(object obj1, object obj2, object obj3, object obj4)
object coalesce(object obj1, object obj2, object obj3, object obj4, object obj5) 
object coalesce(object obj1, object obj2, object obj3, object obj4, object obj5, object obj6)
```

If any of the argument is null and if the function is not designed to handle null, we use the SQL convention and return null.

### Error handling
There are 2 kinds of errors: parse time error and runtime error.

#### Parse time errors
If the ObjectDecorationEx attribute has invalid syntax or one the function does not exist, a exception will be thrown when the sink is loaded.

#### Runtime errors
If the arguments passed to a function is invalid, in order to avoid data loss, the function returns null but a warning message is logged.
