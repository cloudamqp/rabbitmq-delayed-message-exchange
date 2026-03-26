ARG erlang_version=latest
FROM erlang:${erlang_version} AS builder

ARG rebar3_version=3.25.0

WORKDIR /tmp
COPY ../ deps

RUN ls -lisa /tmp

RUN curl -fsSL https://github.com/erlang/rebar3/releases/download/${rebar3_version}/rebar3 -o /usr/local/bin/rebar3 \
    && chmod +x /usr/local/bin/rebar3

WORKDIR /tmp/deps/zstd
RUN rm -f priv/zstd_nif.so c_src/*.o priv/zstd/lib/libzstd.a && rm -rf priv/zstd/lib/obj _build && rebar3 compile

FROM scratch
COPY --from=builder /tmp/deps/zstd/priv/zstd_nif.so .
