defmodule Kanta.CacheTest do
  use ExUnit.Case, async: false

  alias Kanta.Cache
  alias Nebulex.Event.CacheEntryEvent

  setup do
    :ok = Cache.__primary__().subscribe(partition: 0)
  end

  test "a fill stays on this node" do
    key = unique_key()

    from_another_process(fn -> Cache.fill!(key, "value") end)

    assert Cache.get!(key) == "value"
    refute_receive %CacheEntryEvent{}
  end

  test "a fill expires" do
    key = unique_key()

    Cache.fill!(key, "value")

    assert {:ok, ttl} = Cache.ttl(key)
    assert ttl > 0 and ttl <= :timer.hours(1)
  end

  test "a delete reaches the other nodes" do
    key = unique_key()
    Cache.fill!(key, "value")

    from_another_process(fn -> Cache.delete!(key) end)

    assert_receive %CacheEntryEvent{type: :deleted, target: {:key, ^key}}
  end

  test "a local clear empties this node without reaching the others" do
    key = unique_key()
    Cache.fill!(key, "value")

    from_another_process(&Cache.clear_local/0)

    assert Cache.get!(key) == nil
    refute_receive %CacheEntryEvent{}
  end

  test "a full clear reaches the other nodes, even from an empty cache" do
    Cache.clear_local()

    from_another_process(&Cache.clear_all!/0)

    assert_receive %CacheEntryEvent{type: :deleted, target: {:q, _}}
  end

  # The stream broadcasts from the writing process to everyone else, so the
  # write must come from a process other than the subscribed test.
  defp from_another_process(fun), do: fun |> Task.async() |> Task.await()

  defp unique_key, do: "key-#{System.unique_integer([:positive])}"
end
