// use this type for nested graphql objects where not all children are requested in the query
export type DeepPartial<T> = T extends object
    ? {
          [P in keyof T]?: DeepPartial<T[P]>
      }
    : T
