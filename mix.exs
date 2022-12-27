defmodule Sidx.MixProject do
  use Mix.Project

  def project do
    [
      app: :sidx,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [ignore_modules: [
        Sidx.Partition.State,
        Sidx.Table,
        Sidx.Unifier.State
      ]]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Sidx.App, []}
    ]
  end

  defp deps do
    [
      {:libring, "~> 1.6"},
      {:benchee, "~> 1.1", only: :dev},
      {:flow, "~> 1.2", only: :dev}
    ]
  end
end
