# Building TonboLite

### Build as Extension
To build TonboLite as an extension, you should enable loadable_extension features
```sh
cargo build --release --features loadable_extension
```
Once building successfully, you will get a file named libsqlite_tonbo.dylib(`.dll` on windows, `.so` on most other unixes) in *target/release/*
### Build on Rust

```sh
cargo build
```

### Build on Wasm

To use TonboLite in wasm, it takes a few steps to build.
1. Add wasm32-unknown-unknown target
```sh
rustup target add wasm32-unknown-unknown
```
2. Override toolchain with nightly
```sh
rustup override set nightly
```
3. Build with [wasm-pack](https://github.com/rustwasm/wasm-pack)
```sh
wasm-pack build --target web --no-default-features --features wasm
```

Once you build successfully, you will get a *pkg* folder containing compiled js and wasm files. Copy it to your project and then you can start to use it.
```js
const tonbo = await import("./pkg/sqlite_tonbo.js");
await tonbo.default();

// start to use TonboLite ...
```


<div class="warning">

TonboLite should be used in a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts) and [cross-origin isolated](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated), since it uses [`SharedArrayBuffer`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer) to share memory. Please refer to [this article](https://web.dev/articles/coop-coep) for a detailed explanation.

</div>
