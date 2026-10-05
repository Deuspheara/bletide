fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    if os == "android" && matches!(arch.as_str(), "aarch64" | "x86_64") {
        // Rust's target flags can override the NDK's default page alignment.
        // Keep the final shared library loadable on 4 KiB and 16 KiB devices.
        println!("cargo:rustc-link-arg-cdylib=-Wl,-z,max-page-size=16384");
        println!("cargo:rustc-link-arg-cdylib=-Wl,-z,common-page-size=16384");
    }
}
