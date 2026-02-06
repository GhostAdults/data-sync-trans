import 'package:flutter_test/flutter_test.dart';
import 'package:data_trans_desktop_ui/main.dart';
import 'package:data_trans_desktop_ui/src/rust/frb_generated.dart';
import 'package:integration_test/integration_test.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  setUpAll(() async => await RustLib.init());
  testWidgets('Can call rust function', (WidgetTester tester) async {
    await tester.pumpWidget(const DataTransDesktopUI(result: 'Hello, Tom!', configText: '{}', tablesText: '{}'));
    expect(find.textContaining('Result: `Hello, Tom!`'), findsOneWidget);
  });
}
