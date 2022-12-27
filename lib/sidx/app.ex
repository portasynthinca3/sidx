defmodule Sidx.App do
  use Application
  @moduledoc "Sidx OTP app"

  @impl true
  def start(_type, _args) do
    Sidx.Sup.start_link([])
  end
end
