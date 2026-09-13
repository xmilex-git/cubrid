#ifndef _WF259_AUTO_TRACE_HPP_
#define _WF259_AUTO_TRACE_HPP_
#include <cstdio>
#include <cstdlib>
#include <cstdarg>
static void wf259_auto_trace (const char *format, ...)
{
  static thread_local unsigned int count = 0;
  const char *path = std::getenv ("WF259_AUTO_DIAG_FILE");
  if (path == nullptr || count++ >= 96) return;
  FILE *out = std::fopen (path, "a");
  if (out == nullptr) return;
  std::fprintf (out, "[DEBUG-wf259-auto] ");
  va_list args;
  va_start (args, format);
  std::vfprintf (out, format, args);
  va_end (args);
  std::fputc ('\n', out);
  std::fclose (out);
}
#endif
