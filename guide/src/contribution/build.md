# Building Tonbo
To get started using tonbo you should make sure you have [Rust](https://www.rust-lang.org/tools/install) installed on your system. If you haven't alreadly done yet, try following the instructions [here](https://www.rust-lang.org/tools/install).

## Building Tonbo for Rust

To use local disk as storage backend, you should import [tokio](https://github.com/tokio-rs/tokio) crate and enable "tokio" feature (enabled by default)

```bash
cargo build
```

If you want to build tonbo under wasm, you should add wasm32-unknown-unknown target first.

```bash
# add wasm32-unknown-unknown target
rustup target add wasm32-unknown-unknown
# build under wasm
cargo build --target wasm32-unknown-unknown --no-default-features --features wasm
```


## Building Tonbo for Python
We use the [pyo3](https://github.com/PyO3/pyo3) to generate a native Python module and use [maturin](https://github.com/PyO3/maturin) to build Rust-based Python packages.

First, follow the commands below to build a new Python virtualenv, and install maturin into the virtualenv using Python's package manager, pip:

```bash
# setup virtualenv
python -m venv .env
# activate venv
source .env/bin/activate

# install maturin
pip install maturin
# build bindings
maturin develop
```

Whenever Rust code changes run:

```bash
maturin develop
```

## Building Tonbo for JavaScript
To build tonbo for JavaScript, you should install [wasm-pack](https://github.com/rustwasm/wasm-pack). If you haven't already done so, try following the instructions [here](https://rustwasm.github.io/wasm-pack/installer/).

```bash
# add wasm32-unknown-unknown target
rustup target add wasm32-unknown-unknown
# build under wasm
wasm-pack build --target web
```
