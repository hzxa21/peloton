//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions_proxy.cpp
//
// Identification: src/codegen/proxy/string_functions_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/string_functions_proxy.h"

#include "codegen/proxy/type_builder.h"
#include "function/string_functions.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::function, StringFunctions, Ascii);
DEFINE_METHOD(peloton::function, StringFunctions, Like);
DEFINE_METHOD(peloton::function, StringFunctions, Length);

// Trim-related functions
DEFINE_METHOD(peloton::function, StringFunctions, BTrim);
DEFINE_METHOD(peloton::function, StringFunctions, Trim);
DEFINE_METHOD(peloton::function, StringFunctions, LTrim);
DEFINE_METHOD(peloton::function, StringFunctions, RTrim);

// StrWithLen Struct
DEFINE_TYPE(StrWithLen, "peloton::StrWithLen", MEMBER(str), MEMBER(length));

}  // namespace codegen
}  // namespace peloton
