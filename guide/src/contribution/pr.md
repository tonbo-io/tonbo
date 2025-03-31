# Submitting a Pull Request

Thanks for your contribution! The Tonbo project welcomes contribution of various types -- new features, bug fixes and reports, typo fixes, etc. If you want to contribute to the Tonbo project, you will need to pass necessary checks. If you have any question, feel free to start a new discussion or issue, or ask in the Tonbo [Discord](https://discord.gg/j27XVFVmJM).

## Running Tests and Checks
This is a Rust project, so [rustup](https://rustup.rs/) and [cargo](https://doc.rust-lang.org/cargo/) are the best place to start.

- `cargo check` to analyze the current package and report errors.
- `cargo +nightly fmt` to format the current code.
- `cargo build` to compile the current package.
- `cargo clippy` to catch common mistakes and improve code.
- `cargo test` to run unit tests.
- `cargo bench` to run benchmark tests.


> **Note**: If you have any changes to *bindings/python*, please make sure to run checks and tests before submitting your PR. If you don not know how to build and  run tests, please refer to the [Building Tonbo for Python](./build.md#building-tonbo-for-python) section.

## Pull Request title
As described in [here](https://gist.github.com/joshbuchea/6f47e86d2510bce28f8e7f42ae84c716), a valid PR title should begin with one of the following prefixes:
- feat: new feature for the user, not a new feature for build script
- fix: bug fix for the user, not a fix to a build script
- doc: changes to the documentation
- style: formatting, missing semi colons, etc; no production code change
- refactor: refactoring production code, eg. renaming a variable
- test: adding missing tests, refactoring tests; no production code change
- chore: updating grunt tasks etc; no production code change

Here is an example of a valid PR title:
```
feat: add float type
^--^  ^------------^
|     |
|     +-> Summary in present tense.
|
+-------> Type: chore, docs, feat, fix, refactor, style, or test.
```
