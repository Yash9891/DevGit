# =============================================================================
#  PYTHON ERRORS & EXCEPTION HANDLING — Simple Beginner Guide
#  Author  : Yash
#  Purpose : Understand what errors are and how to handle them in Python
# =============================================================================


# -----------------------------------------------------------------------------
# WHAT IS AN ERROR / EXCEPTION?
# -----------------------------------------------------------------------------
# When Python encounters a problem while running your code, it STOPS and
# raises an ERROR (also called an Exception).
#
# There are two kinds of errors in Python:
#
#   1. SYNTAX ERROR   — Mistake in the code structure (typo, missing colon, etc.)
#                       Python catches this BEFORE running the code.
#                       Example: if x == 5    ← missing colon at the end
#
#   2. RUNTIME ERROR  — Happens WHILE the code is running.
#                       Also called an "Exception".
#                       Example: dividing by zero, opening a missing file, etc.
#
# This file focuses on RUNTIME ERRORS and how to handle them gracefully.


# =============================================================================
# COMMON PYTHON ERRORS — What they are and when they happen
# =============================================================================

# ── 1. ZeroDivisionError ─────────────────────────────────────────────────────
# Happens when you try to divide any number by zero.
# result = 10 / 0
# print(result)   # ❌ ZeroDivisionError: division by zero


# ── 2. IndexError ────────────────────────────────────────────────────────────
# Happens when you try to access an index that does not exist in a list/string.
# name = "Yash"
# print(name[20000])   # ❌ IndexError: string index out of range
#                        (name only has 4 characters: index 0 to 3)


# ── 3. ValueError ────────────────────────────────────────────────────────────
# Happens when a function receives the RIGHT TYPE but an INVALID VALUE.
# int("hello")   # ❌ ValueError: invalid literal for int() with base 10: 'hello'
# int("10")      # ✅ This works fine — "10" is a valid number string


# ── 4. TypeError ─────────────────────────────────────────────────────────────
# Happens when an operation is applied to the WRONG DATA TYPE.
# result = 10 + "5"   # ❌ TypeError: unsupported operand type(s) for +: 'int' and 'str'


# ── 5. NameError ─────────────────────────────────────────────────────────────
# Happens when you use a variable that has NOT been defined yet.
# print(marks)   # ❌ NameError: name 'marks' is not defined


# ── 6. FileNotFoundError ─────────────────────────────────────────────────────
# Happens when you try to open a file that does not exist.
# open("missing_file.txt")   # ❌ FileNotFoundError: No such file or directory


# ── 7. KeyError ──────────────────────────────────────────────────────────────
# Happens when you access a dictionary key that does not exist.
# student = {"name": "Yash"}
# print(student["age"])   # ❌ KeyError: 'age'


# ── 8. AttributeError ────────────────────────────────────────────────────────
# Happens when you call a method/attribute that an object does not have.
# x = 5
# x.upper()   # ❌ AttributeError: 'int' object has no attribute 'upper'
#               upper() is a string method, not an int method


# =============================================================================
# EXCEPTION HANDLING — try / except / else / finally
# =============================================================================
#
# Instead of letting errors CRASH your program, you can HANDLE them gracefully
# using a try-except block.
#
# STRUCTURE:
#
#   try:
#       → Write the code that might cause an error here
#
#   except SomeError:
#       → If that specific error occurs, run this block instead of crashing
#
#   else:
#       → Runs ONLY if NO error occurred in the try block
#
#   finally:
#       → ALWAYS runs, whether there was an error or not
#       → Great for cleanup (closing files, disconnecting from databases)
#
# FLOW DIAGRAM:
#   try block runs
#      ├── Error? → except block runs → finally block runs → continue
#      └── No Error? → else block runs → finally block runs → continue


# =============================================================================
# EXAMPLE 1 — Basic try / except
# =============================================================================
# Handling a single specific error type.

print("=" * 45)
print("EXAMPLE 1: Basic try / except")
print("=" * 45)

try:
    result = 10 / 0          # This line causes ZeroDivisionError
    print("Result:", result) # This line is SKIPPED if error occurs
except ZeroDivisionError:
    print("Error: Cannot divide by zero!")  # This runs instead

# Output: Error: Cannot divide by zero!
# Program does NOT crash — it continues normally after the except block.


# =============================================================================
# EXAMPLE 2 — Catching multiple errors separately
# =============================================================================
# You can have multiple except blocks — one for each error type.
# Python checks them top to bottom and runs the FIRST one that matches.

print("\n" + "=" * 45)
print("EXAMPLE 2: Multiple except blocks")
print("=" * 45)

def safe_divide(a, b):
    try:
        result = a / b

    except ZeroDivisionError:
        # Specifically handles division by zero
        print("Error: Division by zero is not allowed!")
        return None

    except TypeError as e:
        # 'as e' captures the error message into variable e
        # Handles when wrong data types are passed (e.g., string instead of number)
        print("Error: Wrong data type!", e)
        return None

    else:
        # Runs ONLY when no exception occurred
        print("Success! Result is:", result)
        return result


safe_divide(10, 0)    # ❌ ZeroDivisionError → except ZeroDivisionError runs
safe_divide(10, "2")  # ❌ TypeError         → except TypeError runs
safe_divide(10, 2)    # ✅ No error          → else block runs

# Output:
# Error: Division by zero is not allowed!
# Error: Wrong data type! unsupported operand type(s) for /: 'int' and 'str'
# Success! Result is: 5.0


# =============================================================================
# EXAMPLE 3 — finally block (always runs)
# =============================================================================
# finally is used for CLEANUP code that must run no matter what.
# Common use: closing files, database connections, releasing resources.

print("\n" + "=" * 45)
print("EXAMPLE 3: finally block")
print("=" * 45)

def read_file(filename):
    try:
        file = open(filename, "r")
        content = file.read()
        print("File content:", content)
    except FileNotFoundError:
        print(f"Error: '{filename}' not found!")
    finally:
        # This block ALWAYS runs — even if an error occurred.
        # Ensures we always print a closing message (or close a file/connection).
        print("Done attempting to read the file.")

read_file("notes.txt")        # File doesn't exist → except + finally both run
# read_file("errors.py")      # This file exists → try + finally both run

# Output:
# Error: 'notes.txt' not found!
# Done attempting to read the file.


# =============================================================================
# EXAMPLE 4 — Catching multiple errors in one except line
# =============================================================================
# If you want the same response for different errors, group them in a tuple.

print("\n" + "=" * 45)
print("EXAMPLE 4: Catching multiple errors together")
print("=" * 45)

def process(value):
    try:
        result = 100 / int(value)
        print("Result:", result)
    except (ZeroDivisionError, ValueError) as e:
        # Handles BOTH errors with the same message
        print("Something went wrong:", e)

#process("0")       # int("0") works, then 100/0 → ZeroDivisionError
process("hello")   # int("hello") fails → ValueError
process("5")       # ✅ No error → prints 20.0


# =============================================================================
# EXAMPLE 5 — Catching ALL exceptions (generic)
# =============================================================================
# Use 'Exception' as a catch-all when you're unsure which error might occur.
# Useful as a safety net, but always prefer specific error types when possible.

print("\n" + "=" * 45)
print("EXAMPLE 5: Generic Exception catch-all")
print("=" * 45)

def risky_operation(x):
    try:
        print(10 / x)
        print("hello"[x])   # IndexError if x is too large
    except Exception as e:
        # 'Exception' catches almost any runtime error
        # 'e' holds the error message
        print(f"An error occurred: {type(e).__name__}: {e}")
        # type(e).__name__ → gives you the name of the error (e.g. "ZeroDivisionError")

#risky_operation(0)    # ZeroDivisionError
risky_operation(100)  # IndexError


# =============================================================================
# EXAMPLE 6 — raise (Triggering your own errors)
# =============================================================================
# You can RAISE (throw) your own exceptions intentionally using the 'raise' keyword.
# This is useful when you want to enforce rules in your code.
# Example: age cannot be negative — there's no built-in Python error for that,
#          so you raise a ValueError yourself.

print("\n" + "=" * 45)
print("EXAMPLE 6: raise — custom error triggering")
print("=" * 45)

def set_age(age):
    if age < 0:
        raise ValueError("Age cannot be negative!")   # manually raise an error
    if age > 150:
        raise ValueError("Age seems unrealistic!")
    print(f"Age set to {age}")

try:
    set_age(-5)   # This will raise ValueError
except ValueError as e:
    print("Caught error:", e)

try:
    set_age(25)   # This is valid
except ValueError as e:
    print("Caught error:", e)


# =============================================================================
# EXAMPLE 7 — Custom Exception Class
# =============================================================================
# You can define your OWN exception types by inheriting from 'Exception'.
# This makes your errors more descriptive and specific to your program.

print("\n" + "=" * 45)
print("EXAMPLE 7: Custom Exception")
print("=" * 45)

class InsufficientBalanceError(Exception):
    # Custom error class for bank account operations
    def __init__(self, balance, amount):
        self.balance = balance
        self.amount  = amount
        # Call parent Exception's __init__ with a descriptive message
        super().__init__(f"Cannot withdraw ₹{amount}. Available balance: ₹{balance}")

def withdraw(balance, amount):
    if amount > balance:
        raise InsufficientBalanceError(balance, amount)   # raise our custom error
    return balance - amount

try:
    new_balance = withdraw(500, 1000)
except InsufficientBalanceError as e:
    print("Bank Error:", e)

try:
    new_balance = withdraw(500, 200)
    print("New balance: ₹", new_balance)
except InsufficientBalanceError as e:
    print("Bank Error:", e)


# =============================================================================
# QUICK REFERENCE — Common Errors Cheat Sheet
# =============================================================================
#
#  Error Name             When it happens
#  ─────────────────────  ──────────────────────────────────────────────────────
#  ZeroDivisionError      Dividing a number by 0
#  IndexError             Accessing an index beyond the list/string length
#  ValueError             Right type, but invalid value  e.g. int("hello")
#  TypeError              Wrong data type for an operation  e.g. "5" + 5
#  NameError              Using a variable that was never defined
#  FileNotFoundError      Opening a file that doesn't exist
#  KeyError               Accessing a dict key that doesn't exist
#  AttributeError         Calling a method an object doesn't have
#
# ─────────────────────────────────────────────────────────────────────────────
#
#  Keyword      Purpose
#  ──────────   ──────────────────────────────────────────────────────────────
#  try          Wrap the code that might fail
#  except       Handle a specific error if it occurs
#  else         Runs only if NO error occurred in try
#  finally      Always runs (error or not) — use for cleanup
#  raise        Manually trigger an exception
#  as e         Capture the error message into variable e


