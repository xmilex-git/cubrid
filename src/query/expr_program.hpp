/*
 * Copyright 2008 Search Solution Corporation
 * Copyright 2016 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

//
// expr_program - flat compiled expression program (PostgreSQL ExprState/ExprEvalStep model)
//
// Shared by client (xasl_generation/xasl_to_stream) and server (stream_to_xasl/fetch/query_aggregate).
// Mode-agnostic: no SERVER_MODE/SA_MODE #error guard so it builds in CS_MODE, SA_MODE, SERVER_MODE.
//

#ifndef _EXPR_PROGRAM_HPP_
#define _EXPR_PROGRAM_HPP_

// forward definitions - only pointers are stored, so heavy headers are avoided
struct db_value;
struct tp_domain;
struct regu_variable_node;

// EXPR_OPCODE - dense step opcode (the integer value is packed in the XASL stream, Phase 4).
// APPEND-ONLY: new opcodes MUST be added at the end before EXPR_OP_LAST; existing values are
// frozen for stream compatibility (P3). Use explicit = 0 start so values stay stable.
typedef enum
{
  EXPR_OP_DONE = 0,		/* program terminator */
  EXPR_OP_QUAL,			/* predicate boolean result + short-circuit (reserved) */
  EXPR_OP_JUMP,			/* control-flow jump (reserved for future) */
  EXPR_OP_CONST,		/* leaf: const value already baked in resval */
  EXPR_OP_VAR,			/* leaf: column/peek value fetch */
  EXPR_OP_MUL,			/* arithmetic multiply */
  EXPR_OP_SUB,			/* arithmetic subtract */
  EXPR_OP_ADD,			/* arithmetic add */
  EXPR_OP_LE,			/* comparison less-or-equal */
  EXPR_OP_CAST,			/* per-tuple column-value cast (literal cast is const-folded at compile) */
  EXPR_OP_FUNC,			/* generic function escape hatch for future opcodes */
  EXPR_OP_LAST			/* sentinel - keep last */
} EXPR_OPCODE;

typedef struct expr_step EXPR_STEP;
typedef struct expr_program EXPR_PROGRAM;

// EXPR_EVAL_CTX - per-tuple execution context threaded by the flat loop to every step (Phase 6).
// Carries the legacy fetch/compare inputs so the step evaluators can reuse fetch_peek_dbval and
// eval_value_rel_cmp verbatim. thread_p/vd/obj_oid/et_comp are void* so this header stays mode- and
// namespace-agnostic (THREAD_ENTRY is cubthread::entry in server, void in CS); the evaluators cast
// them back to their real types (THREAD_ENTRY*, VAL_DESCR*, OID*, const COMP_EVAL_TERM*).
typedef struct expr_eval_ctx EXPR_EVAL_CTX;
struct expr_eval_ctx
{
  void *thread_p;		/* THREAD_ENTRY* */
  void *vd;			/* VAL_DESCR* */
  void *obj_oid;		/* OID* of the current object (leaf fetch) */
  char *tpl;			/* QFILE_TUPLE (list-file leaf fetch); NULL for heap scan */
  EXPR_PROGRAM *program;	/* operand resolution: read steps[arg_idx].resval */
  const void *et_comp;		/* const COMP_EVAL_TERM* - comparison/coercion context for LE */
};

// EXPR_EVAL_FN - per-step evaluator bound at server ready-time.
// NEVER serialized (P4): the pointer is process-local and is rebound from the opcode registry
// after the program is unpacked server-side. Returns a DB_LOGICAL (int) for predicate steps and
// NO_ERROR/ER_FAILED for leaf/arith steps; the flat loop interprets per opcode.
typedef int (*EXPR_EVAL_FN) (EXPR_STEP * step, EXPR_EVAL_CTX * ctx);

// EXPR_STEP ~= PG ExprEvalStep. Target sizeof <= 64B (1 cacheline).
// Operand wiring (PG §3 invariant): a child step writes its result directly into the DB_VALUE
// slot that its parent passed down, so there is NO separate move step. Operands here are PRIOR
// steps referenced by index into EXPR_PROGRAM::step_values (indices serialize cleanly; pointers
// would need re-resolution per clone - so indices are stored and resolved to resval at ready-time).
struct expr_step
{
  EXPR_OPCODE opcode;		/* dense opcode - NOT overwritten with a label (PoC1 uses fn-ptr dispatch) */
  int arg1_idx;			/* first operand: index of a prior step in step_values, or -1 */
  int arg2_idx;			/* second operand: index of a prior step in step_values, or -1 */
  db_value *resval;		/* where this step writes its result (parent's input slot) */
  EXPR_EVAL_FN evaluator;	/* bound at ready-time; NEVER serialized (P4) */

  union
  {
    /* EXPR_OP_VAR / EXPR_OP_CONST leaf: the underlying regu (column or baked literal). Executed by
       reusing the legacy fetch_peek_dbval kernel; serialized via xts pointer-dedup (shared with the
       PRED_EXPR/access-spec copy), rebound server-side at ready-time. Only ARITH dispatch is flattened. */
    regu_variable_node *src;
    struct
    {
      tp_domain *domain;	/* re-resolved server-side; only domain-id is serialized (P4) */
    } cast;
    struct
    {
      int operator_type;	/* CUBRID T_* operator for a generic arith evaluator */
    } arith;
  } d;				/* op-specific inline data, kept minimal (<= ~40B) */
};

#if defined (__cplusplus)
// In C builds porting.h makes static_assert a no-op; guard so the size invariant is actually checked.
static_assert (sizeof (EXPR_STEP) <= 64, "EXPR_STEP must fit in one cacheline (PG keeps ExprEvalStep <= 64B)");
#endif /* __cplusplus */

// strict version (D8): bumped whenever the stream encoding of EXPR_PROGRAM changes; mismatch -> legacy.
#define EXPR_PROGRAM_FORMAT_VERSION 1

/* EXPR_PROGRAM flags - #define (not const int) to keep C linkage clean if a C TU ever includes this */
#define EXPR_PROGRAM_IS_QUAL 0x01	/* program evaluates a predicate (eval_pred path) */

// EXPR_PROGRAM ~= PG ExprState. Contiguous flat program.
struct expr_program
{
  EXPR_STEP *steps;		/* contiguous flat array */
  int steps_len;		/* number of valid steps */
  int steps_alloc;		/* capacity (grow mechanism added in Phase 3) */
  db_value *step_values;	/* per-clone result storage (one per step); allocated in clone xasl_buf arena for parallel safety (P5) */
  EXPR_EVAL_FN program_eval;	/* optional top-level entry; NEVER serialized (rebound at ready-time) */
  unsigned short format_version;	/* set during serialization (D8) = EXPR_PROGRAM_FORMAT_VERSION */
  unsigned short flags;		/* EXPR_PROGRAM_* */
};

// Phase 6 per-opcode evaluators bound into the server registry (stx_Expr_eval_registry). Declared
// here (mode-agnostic) so the registry TU sees them without pulling the server-only query_evaluator.h.
// Defined in query_evaluator.c (reuses fetch_peek_dbval / eval_value_rel_cmp). All including TUs are
// compiled as C++ (CMake LANGUAGE CXX), so default linkage stays consistent with the definitions.
extern int expr_eval_leaf (EXPR_STEP * step, EXPR_EVAL_CTX * ctx);
extern int expr_eval_le (EXPR_STEP * step, EXPR_EVAL_CTX * ctx);

#endif /* _EXPR_PROGRAM_HPP_ */
