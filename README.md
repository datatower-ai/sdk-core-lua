<p align="center">
    <a href="https://datatower.ai/" target="_blank">
        <picture>
            <source srcset="https://dash.datatower.ai/logo_v2.png" media="(prefers-color-scheme: dark)">
            <source srcset="https://dash.datatower.ai/logoWhite_v2.png" media="(prefers-color-scheme: light)" >
            <img src="https://dash.datatower.ai/logoWhite_v2.png" alt="DataTower.ai">
        </picture>
    </a>
</p>

# DataTower.ai - Core - Lua | Server

> This SDK is intended to work with [FileScout]().

## Getting Started
1. Install Prerequisites (See section below).
2. Get the latest [release](https://github.com/datatower-ai/sdk-core-lua/releases/latest).
3. Download `DataTowerSdk.lua`.
4. Download a .so file base on your:
   - Lua interpreter version (lua51, lua52, lua53, lua54, ...),
   - Operating System (Linux, macOS, Windows, ...),
   - CPU architecture (x86_64, aarch64, ...).
5. Rename downloaded .so file to `dt_core_lua.so`
6. Place them at the same directory in the project.
7. Finally, `dt = require("DataTowerSdk")`.

> Feel free to contact us, if no .so file is met your requirements.

## Install Prerequisites
- `luarocks install uuid`
- `luarocks install luasocket`
