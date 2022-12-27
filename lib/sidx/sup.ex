defmodule Sidx.Sup do
  use Supervisor
  @moduledoc "Sidx supervisor"

  def start_link(args), do:
    Supervisor.start_link(__MODULE__, [args], name: __MODULE__)

  def init(_args) do
    children = [
      {Registry, keys: :unique, name: Sidx.Registry.Partitions},
      {Registry, keys: :unique, name: Sidx.Registry.Unifiers},
      {DynamicSupervisor, name: Sidx.TableSup, strategy: :one_for_one}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
