table = Sidx.open!("bench_db", keys: 3)
Agent.start_link(fn -> 0 end, name: :bench_ctr)

safe = false
if not safe, do: Sidx.insert(table, [0, 0, 0], 0) # force load partition

Benchee.run(
  %{
    "insert new" => fn ->
      base = Agent.get(:bench_ctr, & &1)
      Agent.cast(:bench_ctr, Kernel, :+, [1])
      Sidx.insert(table, [base, base + 1, base + 2], base + 3, safe)
    end,
    "insert existing" => fn ->
      base = Agent.get(:bench_ctr, & &1)
      Agent.cast(:bench_ctr, Kernel, :+, [1])
      Sidx.insert(table, [1, 2, 3], base, safe)
    end,

    "select full" => fn -> Sidx.select(table, [1, 2, 3], safe) end,
    "select partial" => fn -> Sidx.select(table, [1, 2], safe) end,

    "update" => fn -> Sidx.update(table, [1, 2, 3], fn {_, v} -> v + 1 end) end,
    "select+insert" => fn ->
      {:ok, [{_, val}]} = Sidx.select(table, [1, 2, 3])
      Sidx.insert(table, [1, 2, 3], val + 1)
    end,
  },
  time: 5,
  memory_time: 2
)

# ops = 100_000
# File.rm_rf("bench_db")
# table = Sidx.open!("bench_db", keys: 2, part_initial: 16)
# start = :erlang.system_time(:millisecond)
# 1..ops
#   |> Flow.from_enumerable
#   |> Flow.map(fn i -> Sidx.insert(table, [i, i + 1], i + 2, true) end)
#   |> Flow.run
# stop = :erlang.system_time(:millisecond)
# dur = stop - start
# "#{ops} ops in #{dur} ms, #{ops / (dur / 1000)} op/s"
