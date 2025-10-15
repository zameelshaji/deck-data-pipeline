# 🎴 DECK Analytics Dashboard

A beautiful, branded analytics dashboard built with Streamlit and Plotly for DECK's business intelligence needs.

![DECK Brand](https://img.shields.io/badge/Brand-DECK%20Pink-%23E91E8C)
![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-1.32-red)

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure database connection
cp .streamlit/secrets.toml.template .streamlit/secrets.toml
# Edit secrets.toml with your database credentials

# 3. Run the dashboard
streamlit run Home.py
```

The dashboard will open at **http://localhost:8501**

---

## 📊 Dashboard Pages

### 🎴 **Home (Executive Overview)**
- Key metrics snapshot
- Growth trends
- AI performance summary
- Content engagement overview

### 📈 **User Analytics**
- Daily/Weekly/Monthly Active Users (DAU/WAU/MAU)
- User acquisition funnel
- Engagement breakdown
- Feature adoption rates

### 🤖 **AI Performance (Dextr)**
- AI query volumes
- Satisfaction rates
- Pack generation metrics
- Quality indicators

### 🏢 **Supplier Portal**
- Individual supplier performance
- Engagement scores
- Conversion funnels
- Sortable performance table

---

## 🎨 Branding

The dashboard features DECK's signature pink branding:
- **Primary Color**: #E91E8C (Magenta Pink)
- **Secondary**: #C7177A (Darker Pink)
- **Success**: #10B981 (Green)
- **Warning**: #F59E0B (Orange)
- **Error**: #EF4444 (Red)

All UI elements, charts, and interactions use this cohesive color scheme.

---

## 📁 Project Structure

```
dashboard/
├── Home.py                          # Main entry point
├── pages/
│   ├── 1_📈_User_Analytics.py       # User metrics
│   ├── 2_🤖_AI_Performance.py       # AI metrics
│   └── 3_🏢_Supplier_Portal.py      # Supplier metrics
├── utils/
│   ├── data_loader.py               # Database queries
│   ├── visualizations.py            # Chart functions
│   └── styling.py                   # DECK branding CSS
├── .streamlit/
│   ├── config.toml                  # Theme config
│   ├── secrets.toml                 # DB credentials (DO NOT COMMIT)
│   └── secrets.toml.template        # Template for secrets
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
└── NEXT_STEPS.md                    # Deployment & enhancement guide
```

---

## 🔧 Configuration

### Database Connection
Edit `.streamlit/secrets.toml`:
```toml
[database]
host = "your-database-host"
port = 5432
database = "your-database-name"
user = "your-username"
password = "your-password"
```

### Theme Customization
Edit `.streamlit/config.toml` to change colors:
```toml
[theme]
primaryColor = "#E91E8C"      # DECK Pink
backgroundColor = "#FFFFFF"    # White
secondaryBackgroundColor = "#F5F5F5"  # Light Gray
textColor = "#1A1A1A"         # Dark Gray
```

---

## 🛠️ Development

### Adding New Metrics
1. Add SQL query to `utils/data_loader.py`
2. Use in your page with `@st.cache_data` decorator
3. Display with `st.metric()` or charts

### Creating New Charts
```python
from utils.visualizations import create_line_chart

fig = create_line_chart(
    df=your_data,
    x='date_column',
    y='value_column',
    title="Your Chart Title",
    y_label="Y Axis Label"
)
st.plotly_chart(fig, use_container_width=True)
```

### Adding New Pages
1. Create `pages/X_🔥_Your_Page.py`
2. Import and apply branding:
```python
from utils.styling import apply_deck_branding, add_deck_footer

st.set_page_config(
    page_title="DECK Analytics - Your Page",
    page_icon="🔥",
    layout="wide"
)

apply_deck_branding()
# ... your content ...
add_deck_footer()
```

---

## 📦 Dependencies

- **Streamlit 1.32**: Web framework
- **Plotly 5.x**: Interactive charts (5.x required for compatibility)
- **Pandas**: Data manipulation
- **psycopg2-binary**: PostgreSQL connector
- **SQLAlchemy**: Database ORM

**Note**: Plotly 6.x has rendering issues with Streamlit 1.32, so we use 5.x

---

## 🚢 Deployment

See [NEXT_STEPS.md](NEXT_STEPS.md) for detailed deployment options:
- Streamlit Community Cloud (free)
- Self-hosted server
- Docker container

**Quick Deploy to Streamlit Cloud**:
1. Push to GitHub
2. Go to https://share.streamlit.io
3. Connect repo and deploy!

---

## 🐛 Troubleshooting

### Charts Not Visible
- **Solution**: Make sure Plotly is version 5.x (`pip install 'plotly>=5.0,<6.0'`)

### Database Connection Failed
- Check `.streamlit/secrets.toml` credentials
- Verify database server is accessible
- Test connection with a SQL client

### Slow Loading
- Add caching with `@st.cache_data(ttl=3600)`
- Optimize SQL queries
- Consider database indexes

### "Module not found" Errors
- Run `pip install -r requirements.txt`
- Ensure you're in the correct directory

---

## 📈 Features

### ✅ Implemented
- Multi-page dashboard with navigation
- Real-time metrics and KPIs
- Interactive charts with tooltips
- Sortable data tables
- CSV export functionality
- Responsive design
- Error handling with helpful messages
- Metric explanations and help text

### 🎯 Enhancement Ideas
- Date range filters
- User authentication
- PDF report exports
- Email alerts
- Real-time auto-refresh
- Mobile optimization
- A/B test analytics
- Cohort analysis

---

## 👥 Contributing

To modify or enhance the dashboard:

1. Create a feature branch
```bash
git checkout -b feature/your-feature
```

2. Make your changes
3. Test thoroughly
4. Commit with descriptive messages
```bash
git commit -m "Add: New supplier analytics view"
```

5. Push and create a pull request

---

## 📄 License

Internal DECK project - All rights reserved

---

## 🆘 Support

For questions or issues:
- Check [NEXT_STEPS.md](NEXT_STEPS.md) for detailed guidance
- Review code comments in utility files
- Check Streamlit docs: https://docs.streamlit.io

---

**Built with ❤️ for DECK Analytics**

Last Updated: 2025-10-15
