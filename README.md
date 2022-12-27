# Subindex
Simple key-value store with subindex support for the BEAM implemented in pure Elixir.

## Installation
```elixir
defp deps do
  [
    {:sidx, "~> 0.1"}
  ]
end
```

## Usage
Example workflow:
```elixir
# loads table at "./path" or creates one if it doesn't exist
table = Sidx.open!("./path", keys: 3)

:ok = Sidx.insert(table, [:a, :b, :c], :d) # the 3 keys and a value
:ok = Sidx.insert(table, [:e, :f, :g], :h)
:ok = Sidx.insert(table, [:e, :f, :i], :j)
:ok = Sidx.insert(table, [:e, :k, :l], :m)

Sidx.select(table, [:a, :b, :c]) # {:ok, [{[], :d}]}
Sidx.select(table, [:e, :f, :g]) # {:ok, [{[], :h}]}
Sidx.select(table, [:e, :f, :i]) # {:ok, [{[], :j}]}
Sidx.select(table, [:e, :f])     # {:ok, [{[:g], :h}, {[:i], :j}]}
Sidx.select(table, [:e, :k])     # {:ok, [{[:l], :m}]}
Sidx.select(table, [:e])         # {:ok, [{[:g, :f], :h}, {[:i, :f], :j}, {[:l, :k], :m}]} <- subkeys in reverse order

# inserts with keys to an existing row overwrite the value
:ok = Sidx.insert(table, [:a, :b, :c], 0)
Sidx.select(table, [:a, :b, :c]) # {:ok, [{[], 0}]}

# update is atomic and more efficient than select+insert
Sidx.update(table, [:a, :b, :c], fn _path, value -> value + 1 end)
Sidx.select(table, [:a, :b, :c]) # {:ok, [{[], 1}]}

Sidx.close!(table)
```
