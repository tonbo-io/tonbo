# FAQ

## Failed to run custom build command for `ring` in macOS
Apple Clang is a fork of Clang that is specialized to Apple's wishes. It doesn't support wasm32-unknown-unknown. You need to download and use llvm.org Clang instead. You can refer to this [issue](https://github.com/briansmith/ring/issues/1824) for more information.

```bash
brew install llvm
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
```
