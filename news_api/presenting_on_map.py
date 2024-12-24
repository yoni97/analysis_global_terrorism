import folium
from configs.mongodb import terrorism_actions

def generate_map():
    rows = terrorism_actions.find({}, {"title": 1, "category": 1, "latitude": 1, "longitude": 1})

    m = folium.Map(location=[20, 0], zoom_start=2)
    for row in rows:
        title = row.get("title", "No Title")
        category = row.get("category", "general")
        lat = row.get("latitude")
        lng = row.get("longitude")

        if lat and lng:
            color = "red" if category == "terror" else "blue"
            folium.Marker(
                location=[lat, lng],
                popup=title,
                icon=folium.Icon(color=color)
            ).add_to(m)

    m.save("news_map.html")
    print("Map saved to news_map.html")

if __name__ == "__main__":
    generate_map()
