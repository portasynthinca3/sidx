defmodule SidxTest do
  use ExUnit.Case

  test "simple KV" do
    File.rm_rf("test_db")
    table = Sidx.open!("test_db", [keys: 1, part_timeout: 100, part_initial: 1, compress: false, slot_size: 512])
    assert Sidx.insert(table, ["hello"], "world") == :ok
    Sidx.close!(table)

    table = Sidx.open!("test_db")
    assert Sidx.select(table, ["hello"]) == {:ok, [{[], "world"}]}
    Sidx.close!(table)
  end

  test "heavy slotting" do
    File.rm_rf("test_db")
    table = Sidx.open!("test_db", [keys: 1, part_timeout: 100, part_initial: 128, compress: false, slot_size: 1])
    for i <- 1..10, do:
      assert Sidx.insert(table, [i], i + 1) == :ok
    Sidx.close!(table)

    table = Sidx.open!("test_db")
    for i <- 1..10, do:
      assert Sidx.select(table, [i]) == {:ok, [{[], i + 1}]}
    Sidx.close!(table)
  end

  test "subkey selection" do
    File.rm_rf("test_db")
    table = Sidx.open!("test_db", [keys: 3, part_timeout: 100, part_initial: 1, compress: false, slot_size: 512])
    for i <- 1..10 do
      assert Sidx.insert(table, [1, i + 1, i + 2], i + 3) == :ok
      assert Sidx.insert(table, [2, i + 2, i + 3], i + 1) == :ok
    end
    Sidx.close!(table)

    table = Sidx.open!("test_db")
    results = for i <- 1..10, do: {[i + 2, i + 1], i + 3}
    {:ok, response} = Sidx.select(table, [1])
    assert MapSet.new(response) == MapSet.new(results)
    Sidx.close!(table)
  end

  test "empty selection" do
    File.rm_rf("test_db")
    table = Sidx.open!("test_db", [keys: 1, part_timeout: 100, part_initial: 1, compress: false, slot_size: 512])
    assert Sidx.select(table, [:a]) == {:ok, []}
    Sidx.close!(table)
  end

  test "update" do
    File.rm_rf("test_db")
    table = Sidx.open!("test_db", [keys: 1, part_timeout: 100, part_initial: 1, compress: false, slot_size: 512])
    assert Sidx.insert(table, [:a], 0) == :ok
    assert Sidx.update(table, [:a], fn _, val -> val + 1 end) == :ok
    assert Sidx.select(table, [:a]) == {:ok, [{[], 1}]}
    Sidx.close!(table)
  end
end
