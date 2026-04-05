// Frida helper for invoking the game's real logout flow.
//
// Binary-backed targets from GameAssembly_dump.dll:
//   MainMenuLogic$$LogOut            RVA 0x0F14D90
//   UserIdent$$LogOut               RVA 0x12454E0
//   SceneLoader$$GoToWelcomeScene   RVA 0x087C740
//
// The cleanest full logout entrypoint is MainMenuLogic$$LogOut:
// it calls UserIdent$$LogOut() and then SceneLoader$$GoToWelcomeScene(),
// which in turn sends the disconnect request and loads the welcome scene.
//
// Usage examples:
//   frida -p <pid> -l logout.js
//   frida -f PixelWorld.exe -l logout.js --no-pause
//
// In the Frida REPL after loading:
//   rpc.exports.logout()

const MAINMENU_LOGOUT_RVA = 0x0F14D90;
const USERIDENT_LOGOUT_RVA = 0x12454E0;
const GOWELCOME_RVA = 0x087C740;

function findGameAssembly() {
    const module = Process.findModuleByName("GameAssembly.dll")
    if (module === null) {
        throw new Error("GameAssembly module not loaded yet.");
    }
    return module;
}

function makeMethodVoidPtr(module, rva) {
    return new NativeFunction(module.base.add(rva), "void", ["pointer"]);
}

function invokeLogout() {
    const module = findGameAssembly();
    const mainMenuLogOut = makeMethodVoidPtr(module, MAINMENU_LOGOUT_RVA);

    console.log("[*] Invoking MainMenuLogic$$LogOut at " + module.base.add(MAINMENU_LOGOUT_RVA));
    // Static-like call pattern observed in the binary: method arg is null.
    mainMenuLogOut(ptr(0));
    console.log("[+] Logout flow invoked.");
}

function invokeUserIdentLogoutOnly() {
    const module = findGameAssembly();
    const userIdentLogOut = makeMethodVoidPtr(module, USERIDENT_LOGOUT_RVA);

    console.log("[*] Invoking UserIdent$$LogOut at " + module.base.add(USERIDENT_LOGOUT_RVA));
    userIdentLogOut(ptr(0));
    console.log("[+] UserIdent logout invoked.");
}

function invokeWelcomeSceneOnly() {
    const module = findGameAssembly();
    const goToWelcomeScene = makeMethodVoidPtr(module, GOWELCOME_RVA);

    console.log("[*] Invoking SceneLoader$$GoToWelcomeScene at " + module.base.add(GOWELCOME_RVA));
    goToWelcomeScene(ptr(0));
    console.log("[+] Welcome-scene transition invoked.");
}

rpc.exports = {
    logout() {
        invokeLogout();
        return "ok";
    },
    useridentlogout() {
        invokeUserIdentLogoutOnly();
        return "ok";
    },
    gotowelcome() {
        invokeWelcomeSceneOnly();
        return "ok";
    },
    autologout() {
        setImmediate(invokeLogout);
        return "scheduled";
    },
};

if (typeof Interceptor !== "undefined") {
    const module = findGameAssembly();
    console.log("[+] Found module: " + module.name + " @ " + module.base);
    console.log("[+] Ready. Call rpc.exports.logout() to invoke the full logout flow.");
    console.log("[+] Optional helpers: rpc.exports.useridentlogout(), rpc.exports.gotowelcome()");
} else {
    console.log("Error: Script not running inside Frida.");
}
