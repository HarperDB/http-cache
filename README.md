# @harperdb/http-cache

A [HarperDB Component](https://docs.harperdb.io/docs/developers/components) for caching HTTP requests/responses from other components.

![NPM Version](https://img.shields.io/npm/v/%40harperdb%2Fhttp-cache)

## Installation
Go into the HarperDB application you would building and install this package and add it to the `config.yaml` file:

1. Install:

```sh
npm install @harperdb/http-cache
```

2. Add to `config.yaml`:

```yaml
'@harperdb/http-cache':
  package: '@harperdb/http-cache'
  files: '/*'
```
## Options

> All configuration options are optional

### `port: number`

Specify a port for the caching server. Defaults to `9926`.
