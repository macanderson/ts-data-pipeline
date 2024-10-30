```mermaid
%%{ init: { 'flowchart': { 'curve': 'monotoneX' } } }%%
graph LR;
option_trades[fa:fa-rocket option_trades &#8205] --> option-trades{{ fa:fa-arrow-right-arrow-left option-trades &#8205}}:::topic;
option-trades{{ fa:fa-arrow-right-arrow-left option-trades &#8205}}:::topic --> option_trade_aggs[fa:fa-rocket option_trade_aggs &#8205];
option_trade_aggs[fa:fa-rocket option_trade_aggs &#8205] --> option-trade-aggs{{ fa:fa-arrow-right-arrow-left option-trade-aggs &#8205}}:::topic;
option-trades{{ fa:fa-arrow-right-arrow-left option-trades &#8205}}:::topic --> market_dashboard[fa:fa-rocket market_dashboard &#8205];


classDef default font-size:110%;
classDef topic font-size:80%;
classDef topic fill:#3E89B3;
classDef topic stroke:#3E89B3;
classDef topic color:white;
```