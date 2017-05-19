module AttributeDefinition = struct
  type t =
    | String of string
    | Number of int
    | Binary of string

  let encoding =
    let open Json_encoding in
    conv
      (function
        | String name -> (name, "S")
        | Number name -> (string_of_int name, "N")
        | Binary name -> (name, "B"))
      (fun (name, typ) -> match typ with
         | "S" -> String name
         | "N" -> Number (int_of_string name)
         | "B" -> Binary name
         | _ -> invalid_arg "AttributeDefinition.encoding")
      (obj2
         (req "AttributeName" string)
         (req "AttributeType" string))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let string s = String s
  let number i = Number i
  let binary s = Binary s
end

module Projection = struct
  type t =
    | All
    | Keys_only
    | Include of string list

  let encoding =
    let open Json_encoding in
    conv
      (function
        | All -> None, Some "ALL"
        | Keys_only -> None, Some "KEYS_ONLY"
        | Include ss -> Some ss, Some "INCLUDE")
      (fun (attrs, typ) -> match attrs, typ with
         | Some attrs, _ -> Include attrs
         | _, Some "KEYS_ONLY" -> Keys_only
         | _, Some "ALL" -> All
         | _ -> invalid_arg "Projection.encoding")
      (obj2
         (opt "NonKeyAttributes" (list string))
         (opt "ProjectionType" string))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let all = All
  let keys_only = Keys_only
  let incl ss = Include ss
end

module ProvisionedThroughput = struct
  type t = {
    r : int ;
    w : int ;
  }

  let encoding =
    let open Json_encoding in
    conv
      (fun { r ; w } -> (r, w))
      (fun (r, w) -> { r ; w })
      (obj2
         (req "ReadCapacityUnits" int)
         (req "WriteCapacityUnits" int))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let create ~r ~w = { r ; w }
end

module KeySchemaElement = struct
  type t =
    | Hash of string
    | Range of string

  let encoding =
    let open Json_encoding in
    conv
      (function
        | Hash name -> name, "HASH"
        | Range name -> name, "RANGE")
      (fun (name, typ) -> match typ with
         | "HASH" -> Hash name
         | "RANGE" -> Range name
         | _ -> invalid_arg "KeySchemaElement.encoding")
      (obj2
         (req "AttributeName" string)
         (req "KeyType" string))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let hash name = Hash name
  let range name = Range name
end

module GlobalSecondaryIndex = struct
  type t = {
    name : string ;
    schema : KeySchemaElement.t list ;
    projection : Projection.t ;
    throughput : ProvisionedThroughput.t ;
  }

  let encoding =
    let open Json_encoding in
    conv
      (fun { name ; schema ; projection ; throughput } ->
         (name, schema, projection, throughput))
      (fun (name, schema, projection, throughput) ->
         { name ; schema ; projection ; throughput })
      (obj4
         (req "IndexName" string)
         (req "KeySchema" (list KeySchemaElement.encoding))
         (req "Projection" Projection.encoding)
         (req "ProvisionedThroughput" ProvisionedThroughput.encoding))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let create ~name ~schema ~projection ~throughput =
    { name ; schema ; projection ; throughput }
end

module LocalSecondaryIndex = struct
  type t = {
    name : string ;
    schema : KeySchemaElement.t list ;
    projection : Projection.t ;
  }

  let encoding =
    let open Json_encoding in
    conv
      (fun { name ; schema ; projection } ->
         (name, schema, projection))
      (fun (name, schema, projection) ->
         { name ; schema ; projection })
      (obj3
         (req "IndexName" string)
         (req "KeySchema" (list KeySchemaElement.encoding))
         (req "Projection" Projection.encoding))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let create ~name ~schema ~projection =
    { name ; schema ; projection }
end

module StreamSpecification = struct
  type view_type =
    | Keys_only
    | New_image
    | Old_image
    | Old_and_new_image

  let view_type_encoding =
    let open Json_encoding in
    string_enum [
      "KEYS_ONLY", Keys_only ;
      "NEW_IMAGE", New_image ;
      "OLD_IMAGE", Old_image ;
      "OLD_AND_NEW_IMAGE", Old_and_new_image ;
    ]

  type t = {
    enabled : bool ;
    view_type : view_type option ;
  }

  let encoding =
    let open Json_encoding in
    conv
      (fun { enabled ; view_type } -> enabled, view_type)
      (fun (enabled, view_type) -> { enabled ; view_type })
      (obj2
         (dft "StreamEnabled" bool false)
         (opt "StreamViewType" view_type_encoding))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let create ?view_type ~enabled () =
    { enabled ; view_type }
end

module CreateTable = struct
  type t = {
    attributes_definitions : AttributeDefinition.t list ;
    global_secondary_indexes : GlobalSecondaryIndex.t list ;
    key_schema : KeySchemaElement.t list ;
    local_secondary_indexes : LocalSecondaryIndex.t list ;
    throughput : ProvisionedThroughput.t ;
    stream_spec : StreamSpecification.t option ;
    name : string ;
  }

  let encoding =
    let open Json_encoding in
    conv
      ( fun { attributes_definitions ; global_secondary_indexes ; key_schema ;
              local_secondary_indexes ; throughput ; stream_spec ; name } ->
        (attributes_definitions, key_schema, throughput, name,
         global_secondary_indexes, local_secondary_indexes, stream_spec))
      (fun (attributes_definitions, key_schema, throughput, name,
            global_secondary_indexes, local_secondary_indexes, stream_spec) ->
        { attributes_definitions ; global_secondary_indexes ; key_schema ;
          local_secondary_indexes ; throughput ; stream_spec ; name })
      (obj7
         (req "AttributesDefinition" (list AttributeDefinition.encoding))
         (req "KeySchema" (list KeySchemaElement.encoding))
         (req "ProvisionedThroughput" ProvisionedThroughput.encoding)
         (req "TableName" string)
         (dft "GlobalSecondaryIndexes" (list GlobalSecondaryIndex.encoding) [])
         (dft "LocalSecondaryIndexes" (list LocalSecondaryIndex.encoding) [])
         (opt "StreamSpecification" StreamSpecification.encoding))

  let pp ?compact ?pp_string ppf t =
    Json_repr.(pp ?compact ?pp_string (module Ezjsonm) ppf t)

  let to_string t =
    Format.asprintf "%a" (pp ?compact:None ?pp_string:None) t

  let create
      ?global_secondary_indexes
      ?local_secondary_indexes
      ?stream_spec
      ~name
      ~attributes_definitions
      ~key_schema
      ~throughput =
    let lsi = match local_secondary_indexes with None -> [] | Some v -> v in
    let gsi = match global_secondary_indexes with None -> [] | Some v -> v in
    {
      attributes_definitions ;
      global_secondary_indexes = gsi;
      key_schema ;
      local_secondary_indexes = lsi ;
      throughput ;
      stream_spec ;
      name ;
    }
end


