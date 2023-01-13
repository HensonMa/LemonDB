## LemonDB: An Multi-threading Database written in C++

## README

### Introduction

This project is a course project of *ECE4820J Introduction to Operating Systems & ECE4821J Introduction to Operating Systems - Attached Lab* offered by [UM-SJTU Joint Institute](https://www.ji.sjtu.edu.cn/) in 22 Fall. 

In this project, we implement an in-memory database `LemonDB` using C++ featured multi-threading.



### Developer Quick Start

See **INSTALL.md** for instructions on building from source.

`ClangFormat` and `EditorConfig` are used to format codes.

Hint on using `ClangFormat`:
`find . -name "*.cpp" -o -name "*.h" | sed 's| |\\ |g' | xargs clang-format -i`

And make sure your code editor has `EditorConfig` support.



### File Structure

- ./src/db
  
  Contains basic source code files for the structure of database and table. 
  
- ./src/multi_thread

  Contains the head file which supports multi threading.

- ./src/query

  Contains multiple database queries. Query level operations are in folder `data` and table level operations are in folder  `management`. 

- ./src/utils

  Contains header files which deal with format and exception.



### More Information

More information is provided in `Wiki`. 

- ***Documentation*** contains detailed design for `Lemondb`.
- ***Manual*** contains instructions for all queries.
- ***Multi Thread Implementation*** contains detailed implementation of multi-threaded `LemonDB`. 


