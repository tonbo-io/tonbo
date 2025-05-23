import { userSchema } from "./schema";
import init, { TonboDB, DbOption } from "./pkg/tonbo_js";

async function main() {
  // Initialize the WASM module
  await init();

  const option = new DbOption("store_dir");
  const db = await new TonboDB(option, userSchema);

  await db.insert({ id: 0, name: "Alice", price: 123.45 });

  const record = await db.get(0, (val) => val);
  console.log("Retrieved record:", record);

  await db.transaction(async (txn) => {
    txn.insert({ id: 1, name: "Bob" });
    const record1 = await txn.get(1, ["id", "price"]);
    const record2 = await txn.get(0, ["id", "price"]);
    console.log(record1);
    console.log(record2);
    // can not read uncommitted change
    const uncommitted_name = await db.get(1, (val) => val.name);
    console.log("read uncommitted name: ", uncommitted_name);
    await txn.commit();
    const name = await db.get(1, (val) => val.name);
    console.log("read committed name: ", name);
  });
}

main().catch(console.error);
