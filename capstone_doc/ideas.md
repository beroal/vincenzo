Ideas for my capstone project for the Rust Bootcamp — Winter 2025.

# Dense passphrase

A program for generating dense passphrases.
A passphrase is an access key in the form of a list of words that humans are supposed to remember and type into a computer.
While passphrases are easier to remember than passwords, they are also longer (entropy values being equal).
Dense passphrases are expected to be approximately two times shorter than usual passphrases with the same entropy.
This is useful if the password length is limited or you want to type passwords faster.
Length shortening is achieved by filtering out frequent letters and taking a fixed-length prefix of each word.
The Rust program will consist of the dictionary builder and the passphrase generator.
The dictionary builder helps users prepare a dictionary and find optimal dictionary parameters.
The passphrase generator randomly and securely generates passphrases for requested entropy using a dictionary.

# BitTorrent media streaming

This is the chosen one. See the [root of this directory](README.md).

# Indented bracketed expressions

A text format based on Lisp S-expressions.
S-expressions are infamous for their large number of parentheses.
Indented bracketed expressions allow to replace parentheses with indentations.
Indented bracketed expressions are suitable for rapidly designing syntax for programming languages, configuration files, rich text, small databases, and network messages.
Other features of the format are:

- **Code in text.** Useful for string interpolation in programming languages and rich text.
- **Minimal.** Syntax constructs that may be parsed further is not included.
- **Defined with formal languages.**

The Rust program will consist of the parser and the pretty printer.
