import { userSchema } from "./schema";
import { TonboDB, DbOption, Bound } from "./pkg/tonbo_js";


const option = new DbOption("store_dir");
const db = await new TonboDB(option, userSchema);

await db.insert({ id: 0, name: "Alice" });

const id = await db.get(0, (val) => val.id);
console.log(id);

await db.transaction(async (txn) => {
  txn.insert({ id: 1, name: "Bob" });
  const record1 = await txn.get(1, ["id"]);
  const record2 = await txn.get(0, ["id"]);
  console.log(record1);
  console.log(record2);
  // can not read uncommitted change
  const uncommitted_name = await db.get(1, (val) => val.name);
  console.log("read uncommitted name: ", uncommitted_name);
  await txn.commit();
  const name = await db.get(1, (val) => val.name);
  console.log("read committed name: ", name);
});


