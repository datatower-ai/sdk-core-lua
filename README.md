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

> **🚧 Work In Progress**

## Getting Started
1. Install Prerequisites (See section below).
2. Get the [latest release](https://github.com/datatower-ai/sdk-core-lua/releases/latest).
3. Download `DataTowerSdk.lua`.
4. Download a .so file according to the lua version installed on the machine.
   - for 5.1: lua51-dt_core_lua.so
   - for 5.x (>= 5.2): dt_core_lua-lua5x.so 
5. Place them at the same directory in the project.
6. Finally, `require("DataTowerSdk")`.

## Install Prerequisites
- `luarocks install uuid`
- `luarocks install lua-cjson`
- `luarocks install luasocket`
