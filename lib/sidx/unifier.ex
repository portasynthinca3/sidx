defmodule Sidx.Unifier do
  use GenServer
  @moduledoc "Combines all partitions of a table into one file"

  alias Task.Supervisor
  require Logger

  defmodule State do
    defstruct [:path, :options, :partitions, :slots, :file]
    @type t :: %__MODULE__{
      path: String.t,
      options: [Sidx.table_option],
      partitions: %{
        non_neg_integer() => {actual_size :: non_neg_integer(), slots :: [non_neg_integer()]}
      },
      slots: non_neg_integer(),
      file: :file.io_device | nil
    }
  end


  @spec child_spec(table :: Sidx.Table.t) :: Supervisor.child_spec()
  def child_spec(table), do: %{
    id: {:unifier, table.path},
    start: {__MODULE__, :start_link, [table]},
    restart: :transient
  }

  def start_link(table), do:
    GenServer.start_link(__MODULE__, table,
      name: {:via, Registry, {Sidx.Registry.Unifiers, table.path}})


  def init(table) do
    Logger.debug("sidx-unifier: starting", table: table.path)

    # for terminate/2 to work
    Process.flag(:trap_exit, true)

    # read state
    state = case File.read(Path.join(table.path, "unifier.etf")) do
      {:ok, bin} ->
        Logger.debug("sidx-unifier: opened", table: table.path)
        :erlang.binary_to_term(bin)

      {:error, :enoent} ->
        Logger.debug("sidx-unifier: creating", table: table.path)
        %State{
          path: table.path,
          options: table.options,
          partitions: %{},
          slots: 0
        }

      {:error, error} ->
        Logger.error("sidx-unifier: failed to read", table: table.path)
        raise ArgumentError, "failed to read unifier: #{inspect error}"
    end

    # open data file
    {:ok, file} = :file.open(Path.join(table.path, "unified.sidx"), [:read, :write, :binary, :raw])
    state = %{state | file: file}

    {:ok, state}
  end


  def terminate(_reason, %State{} = state) do
    Logger.debug("sidx-unifier: stopping", table: state.path)

    # close data file
    :ok = :file.close(state.file)

    # write data
    state = %{state | file: nil}
    bin = :erlang.term_to_binary(state)
    path = Path.join(state.path, "unifier.etf")
    File.write!(path, bin)

    Logger.debug("sidx-unifier: stopped", table: state.path)
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


  def handle_call({:write, num, data}, _from, %State{} = state) do
    # get slot size and partition info
    slot_size = state.options[:slot_size]
    {_actual_size, slots} = Map.get(state.partitions, num, {0, []})

    # determine how many new slots need to be allocated
    overrun = div(byte_size(data), slot_size) + 1 - length(slots)
    Logger.debug("sidx-unifier: write: overrun=#{overrun}", table: state.path, part: num)

    # allocate slots
    {state, slots} = if overrun > 0 do
      # allocate new slots
      first_available = state.slots
      Logger.debug("sidx-unifier: write: allocating slots #{first_available}+#{overrun}", table: state.path, part: num)
      {
        %{state | slots: first_available + overrun},
        slots ++ Enum.into(first_available..first_available + overrun - 1, [])
      }
    else {state, slots} end

    # split data into slots
    {to_write, <<>>} = Enum.map_reduce(slots, data, fn
      slot, <<data::binary-size(slot_size), data_rest::binary>> ->
        {{slot_size * slot, data}, data_rest}
      slot, data ->
        # pad last slot
        trail = slot_size - byte_size(data)
        {{slot_size * slot, <<data::binary, 0::integer-size(trail * 8)>>}, <<>>}
    end)

    # modify partition info
    state = %{state | partitions: Map.put(state.partitions, num, {byte_size(data), slots})}

    # write data and return
    {:reply, :file.pwrite(state.file, to_write), state}
  end


  def handle_call({:read, num}, _from, %State{} = state) do
    # get slot size
    slot_size = state.options[:slot_size]

    case Map.get(state.partitions, num) do
      {actual_size, slots} ->
        # read slots
        to_read = Enum.map(slots, fn slot -> {slot * slot_size, slot_size} end)
        {:ok, slot_list} = :file.pread(state.file, to_read)

        # join and truncate slots
        <<data::binary-size(actual_size), _::binary>>
          = Enum.reduce(slot_list, <<>>, fn slot, acc -> <<acc::binary, slot::binary>> end)

        # write data and return
        {:reply, {:ok, data}, state}

      nil ->
        {:reply, {:error, :no_partition}, state}
    end
  end
end
