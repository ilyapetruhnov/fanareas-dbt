with player as (

    select *,
    row_number() over (partition by id order by id) as rn
    from {{ ref('tm_player') }}

),
player_images as (

    select *
    from {{ ref('tm_player_images') }}
),
final as (
select
    player.id                    
    ,player_image
    ,coalesce(player_images.image, player.player_image) as img
    ,player_name           
    ,player_full_name      
    ,birthplace            
    ,date_of_birth         
    ,date_of_death         
    ,player_shirt_number   
    ,birthplace_country    
    ,age                   
    ,height                
    ,foot                  
    ,international_team    
    ,country               
    ,second_country        
    ,league                
    ,team                  
    ,team_id               
    ,contract_expiry_date  
    ,agent                 
    ,agent_id              
    ,position_group        
    ,player_main_position  
    ,player_second_position
    ,player_third_position 
    ,market_value          
    ,market_value_currency 
    ,market_value_numeral  
    ,market_value_last_change 
    from player
    left join player_images
    on player.id = player_images.id
    where player.rn = 1
)
select * from final






