# Package

version       = "0.1.2"
author        = "jjv360"
description   = "NJSDB - Nim JSON SQLite Database - A simple NoSQL JSON document database"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.0.0"
requires "tiny_sqlite >= 0.2.0"


# Test configuration
task test_basic, "Run basic tests":
  exec "nim c -r tests/tests_basic.nim"

task test_advanced, "Run advanced tests":
  exec "nim c -r tests/tests_advanced.nim"

task test_all, "Run all tests":
  exec "nim c -r tests/tests_basic.nim"
  exec "nim c -r tests/tests_advanced.nim"
