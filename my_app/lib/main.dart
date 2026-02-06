import 'dart:convert';
import 'package:fluent_ui/fluent_ui.dart';
import 'package:window_manager/window_manager.dart';
import 'package:data_trans_desktop_ui/src/rust/api/simple.dart';
import 'package:data_trans_desktop_ui/src/rust/frb_generated.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await windowManager.ensureInitialized();

  await RustLib.init();
  // 初始化并监听配置

  var result = await add(a: 1, b: 2);
  // 获取配置对象
  var configText = getConfig(path: null, id: null);
  // 获取数据库表
  var tablesText = await getTables(name: "test");

  const initialSize = Size(800, 600);
  const minPossibleSize = Size(400, 300);
  
  WindowOptions windowOptions = const WindowOptions(
    size: initialSize,
    minimumSize: minPossibleSize,
    center: true,
    backgroundColor: Colors.transparent,
    skipTaskbar: false,
    titleBarStyle: TitleBarStyle.hidden,
  );
  
  windowManager.waitUntilReadyToShow(windowOptions, () async {
    await windowManager.show();
    await windowManager.focus();
  });
  
  runApp(DataTransDesktopUI(
    result: result.toString(), 
    configText: configText,
    tablesText: tablesText,
    ));
}

class DataTransDesktopUI extends StatelessWidget {
  final String result;
  final String configText;
  final String tablesText;
  
  const DataTransDesktopUI({
    super.key, 
    required this.result, 
    required this.configText,
    required this.tablesText,
  });

  @override
  Widget build(BuildContext context) {
    return FluentApp(
      debugShowCheckedModeBanner: false,
      theme: FluentThemeData(
        accentColor: Colors.teal,
        brightness: Brightness.light,
      ),
      home: MainScreen(
        result: result,
        configText: configText,
        tablesText: tablesText,
      ),
    );
  }
}

class MainScreen extends StatefulWidget {
  final String result;
  final String configText;
  final String tablesText;

  const MainScreen({
    super.key,
    required this.result,
    required this.configText,
    required this.tablesText,
  });

  @override
  State<MainScreen> createState() => _MainScreenState();
}

class _MainScreenState extends State<MainScreen> {
  late String _currentConfigText;
  int _selectedIndex = 0;

  @override
  void initState() {
    super.initState();
    _currentConfigText = widget.configText;
  }

  void _showConfigDialog() async {
    await showDialog(
      context: context,
      builder: (context) => ConfigDialog(
        initialConfigText: _currentConfigText,
        onConfigUpdate: (newText) {
          setState(() {
            _currentConfigText = newText;
          });
        },
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return NavigationView(
      appBar: NavigationAppBar(
        automaticallyImplyLeading: false,
        title: GestureDetector(
          behavior: HitTestBehavior.translucent,
          onPanStart: (details) {
            windowManager.startDragging();
          },
          child: Container(
            color: Colors.transparent,
            alignment: Alignment.centerLeft,
            width: double.infinity,
            height: double.infinity,
            child: const Text(
              'Data Sync Desktop',
              style: TextStyle(fontSize: 14, fontWeight: FontWeight.bold),
            ),
          ),
        ),
        actions: Row(
          mainAxisAlignment: MainAxisAlignment.end,
          children: [
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 16),
              alignment: Alignment.center,
              child: Row(
                children: [
                  const Icon(FluentIcons.calculator_addition, size: 16),
                  const SizedBox(width: 8),
                  Text(
                    'Rust Add Result: ${widget.result}',
                    style: const TextStyle(fontWeight: FontWeight.bold),
                  ),
                ],
              ),
            ),
            const WindowButtons(),
            const SizedBox(width: 12),
          ],
        ),
      ),
      pane: NavigationPane(
        selected: _selectedIndex,
        onChanged: (index) {
          if (index == 1) {
            _showConfigDialog();
          } else {
            setState(() {
              _selectedIndex = index;
            });
          }
        },
        displayMode: PaneDisplayMode.compact,
        items: [
          PaneItem(
            icon: const Icon(FluentIcons.table),
            title: const Text('Tables'),
            body: _buildTablesView(),
          ),
          PaneItem(
            icon: const Icon(FluentIcons.settings),
            title: const Text('Config'),
            body: const SizedBox.shrink(),
          ),
        ],
      ),
    );
  }

  Widget _buildTablesView() {
    List<dynamic> tables = [];
    try {
      tables = jsonDecode(widget.tablesText);
    } catch (e) {
      // 解析失败，显示原始文本
    }

    if (tables.isNotEmpty) {
      return ScaffoldPage(
        header: const PageHeader(title: Text('Database Tables')),
        content: ListView.builder(
          padding: const EdgeInsets.symmetric(horizontal: 24, vertical: 8),
          itemCount: tables.length,
          itemBuilder: (context, index) {
            return Padding(
              padding: const EdgeInsets.only(bottom: 8.0),
              child: Card(
                child: Text(
                  tables[index].toString(),
                  style: const TextStyle(fontSize: 14),
                ),
              ),
            );
          },
        ),
      );
    }

    return ScaffoldPage(
      header: const PageHeader(title: Text('Tables Response')),
      content: Padding(
        padding: const EdgeInsets.all(24.0),
        child: Card(
          child: SelectableText(
            widget.tablesText,
            style: const TextStyle(fontFamily: 'monospace'),
          ),
        ),
      ),
    );
  }
}

class WindowButtons extends StatelessWidget {
  const WindowButtons({super.key});

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        IconButton(
          icon: const Icon(FluentIcons.chrome_minimize),
          onPressed: () => windowManager.minimize(),
        ),
        IconButton(
          icon: const Icon(FluentIcons.chrome_restore),
          onPressed: () async {
            if (await windowManager.isMaximized()) {
              windowManager.unmaximize();
            } else {
              windowManager.maximize();
            }
          },
        ),
        IconButton(
          icon: const Icon(FluentIcons.chrome_close),
          style: ButtonStyle(
            foregroundColor: ButtonState.resolveWith((states) {
              if (states.isHovering) return Colors.white;
              return Colors.black;
            }),
            backgroundColor: ButtonState.resolveWith((states) {
              if (states.isHovering) return Colors.red;
              return Colors.transparent;
            }),
          ),
          onPressed: () => windowManager.close(),
        ),
      ],
    );
  }
}

class ConfigDialog extends StatefulWidget {
  final String initialConfigText;
  final ValueChanged<String> onConfigUpdate;

  const ConfigDialog({
    super.key,
    required this.initialConfigText,
    required this.onConfigUpdate,
  });

  @override
  State<ConfigDialog> createState() => _ConfigDialogState();
}

class _ConfigDialogState extends State<ConfigDialog> {
  late String _configText;

  @override
  void initState() {
    super.initState();
    _configText = widget.initialConfigText;
  }

  void _refresh() {
    var newConfig = getConfig(path: null, id: null);
    setState(() {
      _configText = newConfig;
    });
    widget.onConfigUpdate(newConfig);
  }

  @override
  Widget build(BuildContext context) {
    Map<String, dynamic> configMap = {};
    try {
      configMap = jsonDecode(_configText);
    } catch (e) {
      configMap = {'Error': 'Failed to parse JSON', 'Raw': _configText};
    }

    return ContentDialog(
      title: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          const Text('Configuration'),
          IconButton(
            icon: const Icon(FluentIcons.refresh),
            onPressed: _refresh,
          ),
        ],
      ),
      content: SizedBox(
        height: 400, // Fixed height for scrollable content
        width: 500,
        child: ListView.builder(
          itemCount: configMap.length,
          itemBuilder: (context, index) {
            String key = configMap.keys.elementAt(index);
            dynamic value = configMap[key];
            return _buildConfigItem(context, key, value);
          },
        ),
      ),
      actions: [
        Button(
          child: const Text('Close'),
          onPressed: () => Navigator.pop(context),
        ),
      ],
    );
  }

  Widget _buildConfigItem(BuildContext context, String key, dynamic value) {
    bool isComplex = value is Map || value is List;
    String displayValue = isComplex 
        ? const JsonEncoder.withIndent('  ').convert(value) 
        : value.toString();

    return Padding(
      padding: const EdgeInsets.only(bottom: 8.0),
      child: Expander(
        header: Row(
          children: [
            Icon(
              isComplex ? FluentIcons.folder_open : FluentIcons.settings, 
              size: 16, 
            ),
            const SizedBox(width: 8),
            Text(key, style: const TextStyle(fontWeight: FontWeight.bold)),
          ],
        ),
        content: SelectableText(
          displayValue,
          style: TextStyle(
            fontFamily: isComplex ? 'monospace' : null,
          ),
        ),
      ),
    );
  }
}
