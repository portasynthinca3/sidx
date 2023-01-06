defmodule Sidx.MixProject do
  use Mix.Project

  def project do
    [
      app: :sidx,
      version: "0.1.5",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      description: "Simple key-value store with subindex support for the BEAM implemented in pure Elixir",
      deps: deps(),
      package: package(),
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
      {:benchee, "~> 1.1", only: :dev, runtime: false},
      {:flow, "~> 1.2", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      name: :sidx,
      files: ["lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["portasynthinca3"],
      licenses: ["WTFPL"],
      links: %{"GitHub" => "https://github.com/portasynthinca3/sidx"}
    ]
  end
end
