defmodule Sidx.Partition do
  use GenServer
  @moduledoc """
  Handles requests to one partition within a table
  """

  require Logger

  @type tree :: %{term() => tree() | reference()}

  defmodule State do
    defstruct [:path, :num, :timeout, :data, :opts]
    @type t :: %__MODULE__{
      path: String.t,
      num: non_neg_integer(),
      timeout: timeout(),
      data: {tree :: Sidx.Partition.tree, values :: %{reference() => term()}},
      opts: [Sidx.table_option()]
    }
  end


  @spec start(table :: Sidx.Table.t, num :: non_neg_integer()) :: DynamicSupervisor.on_start_child()
  def start(%Sidx.Table{} = table, num), do:
    Supervisor.start_child(table.sup, %{
      id: {table.path, num},
      start: {__MODULE__, :start_link, [{table.path, num, table.options}]},
      restart: :transient
    })

  def start_link({path, num, _opts} = arg), do: GenServer.start_link(__MODULE__, arg,
    name: {:via, Registry, {Sidx.Registry.Partitions, {path, num}}})


  @doc "Returns the pid (safe) or name (unsafe but fast) of a partition"
  @spec get_process(table :: Sidx.Table.t, primary_key :: term(), safe :: boolean()) :: pid() | {:via, term(), term()}
  def get_process(table, primary_key, _safe = false) do
    # use the process dictionary to cache pids
    num = HashRing.key_to_node(table.ring, primary_key)
    key = {table.path, num}
    cache = Process.get(:sidx_cache, %{})

    case Map.get(cache, key) do
      nil ->
        pid = get_process(table, primary_key, true)
        cache = Map.put(cache, key, pid)
        Process.put(:sidx_cache, cache)
        pid

      pid -> pid
    end
  end

  def get_process(table, primary_key, _safe = true) do
    num = HashRing.key_to_node(table.ring, primary_key)
    case start(table, num) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
      {:error, :already_present} ->
        {:ok, pid} = Supervisor.restart_child(table.sup, {table.path, num})
        pid
      {:error, err} ->
        Logger.error("sidx: failed to start partition: #{inspect err}", table: table.path, part: num)
    end
  end


  def init({path, num, opts}) do
    Logger.debug("sidx: opening partition", table: path, part: num)

    # for terminate/2 to work
    Process.flag(:trap_exit, true)

    # read partition data
    unifier = {:via, Registry, {Sidx.Registry.Unifiers, path}}
    data = case GenServer.call(unifier, {:read, num}) do
      {:ok, bin} ->
        bin = if opts[:compress], do: :zlib.gunzip(bin), else: bin
        Logger.debug("sidx: partition opened", table: path, part: num)
        :erlang.binary_to_term(bin)

      {:error, :no_partition} ->
        Logger.debug("sidx: creating partition", table: path, part: num)
        {%{}, %{}}
    end

    # create state
    timeout = opts[:part_timeout]
    {:ok, %State{
      path: path,
      num: num,
      timeout: timeout,
      data: data,
      opts: opts
    }, timeout}
  end


  def terminate(_reason, %State{} = state) do
    Logger.debug("sidx: closing partition", table: state.path, part: state.num)

    # write partition data
    unifier = {:via, Registry, {Sidx.Registry.Unifiers, state.path}}
    bin = :erlang.term_to_binary(state.data)
    bin = if state.opts[:compress], do: :zlib.gzip(bin), else: bin
    :ok = GenServer.call(unifier, {:write, state.num, bin})

    Logger.debug("sidx: partition closed", table: state.path, part: state.num)
  end


  def handle_info({:EXIT, pid, reason}, state) do
    cond do
      pid == self() ->
        {:stop, reason, state}
      reason == :normal or reason == :shutdown ->
        {:noreply, state, state.timeout}
      true ->
        {:stop, reason, state}
    end
  end

  def handle_info(:timeout, state), do: {:stop, :shutdown, state}


  @doc "Looks up or creates a reference in the tree for a list of secondary keys"
  @spec find_slot(tree :: tree(), keys :: [term()], create :: boolean()) :: {tree(), reference() | nil}
  def find_slot(tree, [key], create) do
    case Map.get(tree, key) do
      nil when create ->
        ref = make_ref()
        {Map.put(tree, key, ref), ref}
      nil ->
        {tree, nil}
      ref ->
        {tree, ref}
    end
  end

  def find_slot(tree, [key|rest], create) do
    subtree = Map.get(tree, key, %{})
    {subtree, slot} = find_slot(subtree, rest, create)
    if create, do: {Map.put(tree, key, subtree), slot}, else: {tree, slot}
  end


  @doc "Collects all slots of a tree to a list"
  @spec collect_slots(subtree :: tree() | reference(), path :: [term()]) :: [reference()]
  def collect_slots(subtree, path \\ [])
  def collect_slots(leaf, path) when is_reference(leaf), do: [{path, leaf}]
  def collect_slots(subtree, path) when is_map(subtree), do:
    Enum.flat_map(subtree, fn {k, v} -> collect_slots(v, [k|path]) end)


  def handle_call({:insert, keys, value}, _from, %State{} = state) do
    {tree, values} = state.data
    {tree, slot} = find_slot(tree, keys, true)

    state = %{state | data: {tree, Map.put(values, slot, value)}}

    {:reply, :ok, state, state.timeout}
  end


  def handle_call({:select, keys}, _from, %State{} = state) do
    {tree, values} = state.data

    result = case find_slot(tree, keys, false) do
      {_, nil} -> []

      {_, subtree} ->
        slots = collect_slots(subtree)
        Enum.map(slots, fn {path, key} -> {path, Map.get(values, key)} end)
    end

    {:reply, {:ok, result}, state, state.timeout}
  end


  def handle_call({:update, keys, fun}, _from, %State{} = state) do
    {tree, values} = state.data
    state = case find_slot(tree, keys, false) do
      {_, nil} -> state

      {_, subtree} ->
        slots = collect_slots(subtree)

        values = Enum.reduce(slots, values, fn {path, key}, acc ->
          val = Map.get(values, key)
          updated_val = fun.(path, val)
          Map.put(acc, key, updated_val)
        end)

        %{state | data: {tree, values}}
    end

    {:reply, :ok, state, state.timeout}
  end
end
