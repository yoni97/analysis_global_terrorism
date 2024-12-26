def get_average_wounded_by_region(show_top_5=False):
    try:
        # העלאת הנתונים ל-DataFrame
        df = upload_to_pandas(terrorism_actions)

        # המרת עמודות למספרים וטיפול בחסרים
        for col in ['killed', 'wounded', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])

        # חישוב סך הנפגעים
        df['total_wounded'] = df['killed'] + df['wounded']

        # חישוב ממוצע הנפגעים לפי אזור
        region_stats = df.groupby('region').agg(
            avg_wounded=('total_wounded', 'mean'),
            total_wounded=('total_wounded', 'sum'),
            event_count=('region', 'size'),
            latitude=('latitude', 'mean'),  # ממוצע הקורדינטות לאזור
            longitude=('longitude', 'mean')
        ).reset_index()

        # חישוב אחוז הנפגעים מכלל האירועים באותו אזור
        region_stats['percentage_wounded'] = region_stats['total_wounded'] / region_stats['event_count']

        # סינון ל-5 העליונים אם נדרש
        if show_top_5:
            region_stats = region_stats.nlargest(5, 'avg_wounded')

        # יצירת מפה עם folium
        folium_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=2)

        # הוספת מעגלים לפי האזורים
        for _, row in region_stats.iterrows():
            color = 'red' if row['avg_wounded'] > 50 else 'blue'
            fill_color = 'orange' if row['avg_wounded'] > 50 else 'lightblue'
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=row['avg_wounded'] / 10,  # קוטר המעגל תלוי בממוצע הנפגעים
                color=color,
                fill=True,
                fill_color=fill_color,
                fill_opacity=0.6,
                popup=f"Region: {row['region']}<br>Avg Wounded: {row['avg_wounded']:.2f}<br>Total Wounded: {row['total_wounded']}<br>Percentage Wounded: {row['percentage_wounded']:.2%}"
            ).add_to(folium_map)

        # שמירת המפה
        folium_map.save('templates/average_wounded_by_region_map.html')

        map_html = folium_map._repr_html_()

        results = region_stats[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(orient='records')
        print("Results:", results)
        return results, map_html

    except Exception as e:
        logging.error(f"Error in get_average_wounded_by_region: {str(e)}")
        return {"status": "error", "message": str(e)}
