defmodule Kanta.Cache do
  @moduledoc """
  Kanta Cache for minimalizing calls to DB

  Coherent: every node keeps its own copy and reads it locally, so a lookup
  never leaves the node and never depends on which nodes are connected.

  Only deletes are broadcast. A fill stays on the node that read it from the
  database, so nodes filling the same key do not evict each other. A write
  that changes a translation deletes its key instead of putting the new value,
  and every node refills from the database on its next read.

  A node that was disconnected when a delete was broadcast never receives it.
  `clear_local/0` drops this node's copy without telling the others, for the
  host to call when it reconnects to a node that may have changed
  translations. Fills expire after an hour, which caps how long anything else
  that goes missing can serve an old translation.
  """

  use Nebulex.Cache,
    otp_app: :kanta,
    adapter: Nebulex.Adapters.Coherent,
    adapter_opts: [primary_storage_adapter: Nebulex.Adapters.Local]

  alias Nebulex.Event.CacheEntryEvent

  @fill_ttl :timer.hours(1)

  @impl Nebulex.Cache
  def init(config) do
    stream_opts = [partitions: 1, hash: &__MODULE__.invalidation_partition/1]

    {:ok, Keyword.update(config, :stream_opts, stream_opts, &Keyword.merge(&1, stream_opts))}
  end

  @doc """
  Caches a value read from the database, on this node only.
  """
  def fill!(key, value), do: put!(key, value, ttl: @fill_ttl)

  @doc """
  Empties the cache on every node.

  Nebulex reports a `delete_all` only when it deleted something on this node,
  so an empty cache here would invalidate no other node. The entry put first
  guarantees there is something to delete; being a fill, it is never
  broadcast itself.
  """
  def clear_all! do
    put!(:clear_all, true)
    _ = delete_all!()
    :ok
  end

  @doc """
  Empties this node's cache without invalidating any other node.
  """
  def clear_local do
    _ = __primary__().delete_all!([], telemetry_metadata: %{local: true})
    :ok
  end

  @doc """
  The stream's `:hash`: broadcasts deletes, except `clear_local/0`'s, and
  discards every other event.
  """
  def invalidation_partition(%CacheEntryEvent{
        type: :deleted,
        metadata: %{extra_metadata: %{local: true}}
      }),
      do: :none

  def invalidation_partition(%CacheEntryEvent{type: :deleted}), do: 0
  def invalidation_partition(%CacheEntryEvent{}), do: :none

  def generate_cache_key(prefix, params) do
    Enum.reduce(params, prefix, fn {key, value}, acc ->
      case value do
        val when is_binary(val) ->
          acc <> "_" <> to_string(key) <> "_" <> URI.encode_query(val)

        val when is_list(val) ->
          # this old way is just broken for preloads lists
          # encoded_list = (Enum.into(val, %{}) |> URI.encode_query())
          # the new way is robust and reversible
          encoded_list =
            val
            |> :erlang.term_to_binary()
            |> URI.encode()
            |> then(&%{encoded_params: &1})
            |> URI.encode_query()

          acc <> "_" <> to_string(key) <> "_" <> encoded_list

        _val ->
          acc
      end
    end)
  end
end
