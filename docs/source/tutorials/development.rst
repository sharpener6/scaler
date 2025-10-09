=====================
Development Guideline
=====================

.. contents:: Table of Contents
    :depth: 2

Please be aware that to contribute the code in this repo, a lot rules are opionated, if you don't follow the guideline,
likely the pull request will not get accepted

The development guideline will keep updated if some the undefined rules are missing or some rules are not good

This project is consists of both python guidelines and C++ code guidelines, as they are hybrids

This coding guideline should be used by both Human and AI to follow.

Common Guideline
----------------

Naming
~~~~~~
Names should be crystal clear inside of the code context, context can be function, module(file), library
In general names should be as specific as possible, unless the names are conventional

Bad examples:
 * data1, data2
 * a, b, c, d, e
 * plain df
 * function1
 * array

Please also avoid abbreviations as much as possible, abbreiations make the code unreadable and difficult to debug and
maintain, unless they are well known, or it's common sense in the industry

Numbers
~~~~~~~
Constant numbers should not be in the code, all constant number should have a variable name, which tells you what is it
doing, in short, magical number is not allowed

Object Oriented Programming (OOP)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OOP is a feature in both C++ and Python language, people should use OOP, We encourage people to use class to
encapsulate the members if those variables should come together.

**We allow subtyping, But we don't allow inheritance**
 * subtyping: allowed, override member functions that has no implementatons (it called interface in c++)
 * inheritance: disallowed, means overriding function that has implmentations

Any class method, or static method in python is allowed, but class method has no difference from module function, class
method or module method should be defined in the right scope if it's tied to class or module

Functions
~~~~~~~~~

 * please refactoring the code as much as possible, except for the test cases, we can allow some duplication if the test
   cases contain same or similar example inputs, but the utility functions used by unit test should still be refactored
 * complex if condition filtering logic should be written in guard clauses way

Python Guideline
----------------

 * We follow PEP8 very closely, only exception is we use line width 120 instead of 80, as the coding style in PEP8 is
   very well defined, I will not spend time here to explain, please google and search on PEP8
 * Python type hinting is mandatory for all function arguments and return type, and it will be used by MyPy to check
 * We use black formatter, the command line arguments are `black -l 120 -C`
 * all python imports should be absolute import
 * we don't allow any content in `__init__.py` unless special cases like project root `__init__.py` for library user
   convenience

Test Cases
~~~~~~~~~~

 * Python test cases should use standard python unittest module, and each class should be one python file and named with
   prefix `test_*.py`
 * unit test structure should be mirror to code structure as tests directory is in the root repo directory

C++ Guideline
-------------

Naming
~~~~~~

Please use CamelCase for naming:
 * for namespace, classes, please capitalize first charactor, e.g.:
   * SomeClass
   * LongNameClass
 * for global variables, or macro, use all capitalized characters, for system or thrid party library definitions, are
   exempt from this rule:
   * GLOBAL_VARIABLE
 * for variable, function names, please use camelCase, with first charactor lower case:
   * addSomeValue
   * deleteSomeValue
 * for the abbreviation word, we should name them as all capitalized, e.g.:
   * use TCPAcceptor instead of TcpAcceptor
   * use IOSocket instead of IoSocket

The file names should be snake cased, for headers please use extention .h, for sources, please use extention .cpp:

 * header files: message_connection.h
 * source files: message_connection.cpp

Formatting
~~~~~~~~~~

Please use `.clang-format` on the root repo directory for code formatting

Name Space
~~~~~~~~~~

 * please don't use using namespace like `using namespace std;`
 * Please always use prefixed namespace like `std::cout`

Includes
~~~~~~~~

 * please remove all the includes files if no functions get used
 * please always include the header files as long as current file is used some function from that header file, indirect
   include is not good, for example:
   * there files `common.h`, `some_module.h`, `application.cpp`
   * at the top of `common.h`, it has line for `#include <cstring>`
   * at the top of `some_module.h`, it has line for `#include "common.h"`
   * at the top of `application.cpp`, it has line `#include <some_module.h>`
   * but `application.cpp` uses function in `cstring`, even it can compile because it indirectly includes `<cstring>`
     because of `common.h`, this is not good, please explicitly include `cstring` in `application.cpp`

Struct/Class
~~~~~~~~~~~~

In C++, struct and class are basically the same, except the default private/public permission. please use `struct` if
you want use data structure with minimal helper functions, use class if it's stateful data structure, don't directly
expose fields when use class, always use setter and getter.

.. code:: cpp
    struct Address {
        std::string domain;
        int port;
    };

    // If data structure has internal states not meant for outsider to use, provide methods that changes states, instead
    // of direct expose structure

    class Client {
    public:
        void send(const std::vector<uint8_t>& buffer);

        int get_message_count();

    private:
        int message_count;
    };


Code Layout
~~~~~~~~~~~

For any h/cpp file, the sequence should be:
 * system libraries includes
 * standard libraries includes
 * third party libraries includes
 * in library includes
 * global variables
 * API classes and functions
 * internal functions that not meant to be used outside of the file

the class definition should be in following order:

.. code:: cpp
    // in the class, it should be always first public, then protected, then private sections
    // the keyword using, should be always in the top then contructors copy/move constructor, destructor
    // the function definitions should come first, then member variables
    class Foobar {
    public:
        // using types should go first
        using Type = namespace::some_type;

        Foobar();
        Foobar(const std::string& foobar) _foobar(foobar);
        ~Foobar();

        const std::string& getCurrentFoobar();

    protected:
        void someProtectedMethod();

        int _SomeProtectedVariable;

    private:
        void SomePrivateMethod();

        int _SomePrivateVariable;
    };
