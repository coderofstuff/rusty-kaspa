import * as kaspa from './kaspa/kaspa_wasm';
import {
    Mnemonic,
    XPrv,
    XPub,
    DerivationPath
} from './kaspa/kaspa_wasm';

kaspa.initConsolePanicHook();

(async () => {

    const mnemonic: Mnemonic = Mnemonic.random();
    console.log("mnemonic:", mnemonic);
    const seed = mnemonic.toSeed("my_password");
    console.log("seed:", seed);

    // ---

    const xPrv: XPrv = new XPrv(seed);
    console.log("xPrv", xPrv.intoString("xprv"))

    console.log("xPrv", xPrv.derivePath("m/1'/2'/3").intoString("xprv"))

    const path: DerivationPath = new DerivationPath("m/1'");
    path.push(2, true);
    path.push(3, false);
    console.log(`path: ${path}`);

    console.log("xPrv", xPrv.derivePath(path).intoString("xprv"))

    const xPub: XPub = xPrv.publicKey();
    console.log("xPub", xPub.derivePath("m/1").intoString("xpub"));
})();
