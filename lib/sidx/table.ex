defmodule Sidx.Table do
  @moduledoc "Internal sidx struct"

  defstruct [
    options: [],
    ring: nil,
    path: nil,
    sup: nil
  ]

  @type t :: %__MODULE__{
    options: [Sidx.table_option],
    ring: HashRing.t | nil,
    path: String.t,
    sup: pid()
  }
end
