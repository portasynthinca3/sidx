defmodule Sidx do
  @moduledoc """
  Subindex public API
  """

  alias Sidx.{Table, Partition, Unifier}
  require Logger

  @typedoc """
  Table options:
    - `keys`: the number of keys that together map to one value, forming one row
    - `part_size`: max number of rows in one partition ()
    - `part_timeout`: the time (in ms) partitions are held in memory for
    - `part_initial`: the number of initial partitions
    - `compress`: compress partitions using gzip (boolean)
    - `slot_size`: slot size (bytes) in the unified partition file
  """
  @type table_option() ::
    {:keys, pos_integer()} |
    {:part_size, pos_integer()} |
    {:part_timeout, pos_integer()} |
    {:part_initial, pos_integer()} |
    {:compress, boolean()} |
    {:slot_size, pos_integer()}

  defp default_options, do: [
    part_size: 100_000,
    part_timeout: 2500,
    part_initial: 1,
    compress: true,
    slot_size: 1024 * 10
  ]

  @doc """
  Attempts to open table at `path` or create one with `opts` if it doesn't exist
  """
  @spec open!(path :: String.t, opts :: [table_option()]) :: Table.t | no_return()
  def open!(path, opts \\ []) do
    Logger.debug("sidx: opening table", table: path)

    # assign default options
    opts = Keyword.merge(default_options(), opts)

    # attempt to read table header
    table = case File.read(Path.join(path, "header.etf")) do
      {:ok, data} ->
        Logger.debug("sidx: loaded table header", table: path)
        :erlang.binary_to_term(data)

      {:error, :enoent} ->
        File.mkdir_p(path)
        Logger.debug("sidx: creating new table", table: path)

        # check options
        if opts[:keys] == nil, do: raise ArgumentError, "option :keys should be set"
        if opts[:part_size] < 1, do: raise ArgumentError, "option :part_size should be >= 1"
        if opts[:part_initial] < 1, do: raise ArgumentError, "option :part_initial should be >= 1"

        # populate ring with initial partitions
        ring = Enum.reduce(0..opts[:part_initial] - 1, HashRing.new(),
          fn idx, ring -> HashRing.add_node(ring, idx, 1) end)

        # create header
        table = %Table{
          options: opts,
          ring: ring,
          path: path
        }

        # write header
        File.write!(Path.join(path, "header.etf"), :erlang.term_to_binary(table))
        table

      {:error, error} ->
        Logger.error("sidx: failed to read header", table: path)
        raise ArgumentError, "failed to read header: #{inspect error}"
    end

    # start table supervisor with its unifier under the main supervisor
    {:ok, sup} = DynamicSupervisor.start_child(Sidx.TableSup, %{
      id: {:table, path},
      start: {Supervisor, :start_link, [[Unifier.child_spec(table)], [strategy: :one_for_one]]},
      restart: :transient
    })
    table = %{table | sup: sup}

    table
  end



  @doc """
  Attempts to perform maintenance tasks on and close `table`
  """
  @spec close!(table :: Sidx.Table.t) :: :ok
  def close!(table) do
    Logger.debug("sidx: closing table", table: table.path)

    # do maintenance
    {_, table} = maintain(table)

    # close all partitions
    DynamicSupervisor.stop(table.sup)

    # write header
    table = table |> Map.put(:sup, nil)
    data = :erlang.term_to_binary(table)
    path = Path.join(table.path, "header.etf")
    File.write!(path, data)

    Logger.debug("sidx: table closed", table: table.path)
    :ok
  end



  @doc """
  Performs maintenance tasks (such as repartitioning) on a table. This is done
  automatically by `close!/1`. Performing operations on the table while it's
  being maintained leads to to undefined behavior.
  """
  @spec maintain(table :: Sidx.Table.t) :: {[term()], Sidx.Table.t}
  def maintain(table) do
    # TODO
    tasks = []
    Logger.debug("sidx: performed maintenance: #{inspect tasks}", table: table.path)
    {tasks, table}
  end



  @doc """
  Inserts one row into the table. Setting `safe` to `false` speeds up the
  execution, but is only actually safe if the time since the last operation is
  less than the configured partition unload timeout and the correct number of
  keys is provided
  """
  @spec insert(table :: Sidx.Table.t, keys :: [term()], value :: term(), safe :: boolean()) :: :ok | {:error, term()}
  def insert(table, keys, value, safe \\ true) do
    # check key count
    if safe and length(keys) != table.options[:keys], do:
      raise ArgumentError, "invalid number of keys: got #{length(keys)}, #{table.options[:keys]} required"

    # determine the partition and ask it to write the row
    [primary | _] = keys
    part = Partition.get_process(table, primary, safe)
    GenServer.call(part, {:insert, keys, value})
  end



  @doc """
  Selects rows from the table. Setting `safe` to `false` speeds up the
  execution, but is only actually safe if the time since the last operation is
  less than the configured partition unload timeout and at least one key is
  provided
  """
  @spec select(table :: Sidx.Table.t, keys :: [term()], safe :: boolean()) :: {:ok, [term()]} | {:error, term()}
  def select(table, keys, safe \\ true) do
    # check key count
    if safe and (length(keys) < 1 or length(keys) > table.options[:keys]), do:
      raise ArgumentError, "invalid number of keys: got #{length(keys)}; min 1, max #{table.options[:keys]}"

    # determine the partition and ask it to write the row
    [primary | _] = keys
    part = Partition.get_process(table, primary, safe)
    GenServer.call(part, {:select, keys})
  end



  @doc """
  Selects rows from the table, applies `fun` to each row and writes the results back.
  Setting `safe` to `false` speeds up the execution, but is only actually safe if
  the time since the last operation is less than the configured partition unload
  timeout and at least one key is provided
  """
  @spec update(table :: Sidx.Table.t, keys :: [term()], fun :: ([term()], term() -> term()), safe :: boolean()) :: :ok | {:error, term()}
  def update(table, keys, fun, safe \\ true) do
    # check key count
    if safe and (length(keys) < 1 or length(keys) > table.options[:keys]), do:
      raise ArgumentError, "invalid number of keys: got #{length(keys)}; min 1, max #{table.options[:keys]}"

    # determine the partition and ask it to update the rows
    [primary | _] = keys
    part = Partition.get_process(table, primary, safe)
    GenServer.call(part, {:update, keys, fun})
  end
end
