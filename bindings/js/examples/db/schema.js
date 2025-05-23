export const userSchema = {
  id: {
    primary: true,
    type: "UInt8",
    nullable: false,
  },
  name: {
    type: "String",
    nullable: true,
  },
  price: {
    type: "Float64",
    nullable: true,
  },
};
